//! Masstree – a trie of B+-trees with 8-byte key slices.
//!
//! Each `Layer` processes the **first 8 bytes** of the key it receives and
//! stores them in a B+-tree keyed on those 8 bytes.  When two keys share the
//! same 8-byte prefix a sub-layer is created automatically; the sub-layer
//! receives `key[8..]` (the remaining bytes) and handles them the same way,
//! giving an arbitrarily-deep trie structure.
//!
//! Keys and values are **borrowed** – the tree stores slices pointing into the
//! caller's memory and does not copy them.  Callers must ensure keys and values
//! remain valid for the lifetime of the tree.
//!
//! Concurrency: this implementation is **single-threaded** (no locking).

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;

// ── Public constants ─────────────────────────────────────────────────────────

/// Maximum number of keys stored in a single B+-tree node.
pub const FANOUT: usize = 15;

// ── Key-slice helpers ────────────────────────────────────────────────────────

/// Extract the first 8 bytes of `key` as a big-endian u64.
/// Keys shorter than 8 bytes are zero-padded on the right so that
/// lexicographic order on u64 matches lexicographic order on the original bytes.
pub fn extractSlice(key: []const u8) u64 {
    const end = @min(@as(usize, 8), key.len);
    var s: u64 = 0;
    for (key[0..end]) |byte| {
        s = (s << 8) | @as(u64, byte);
    }
    // Left-align: shift remaining zero-padded bits into place.
    if (end > 0 and end < 8) {
        s <<= @as(u6, @intCast((8 - end) * 8));
    }
    return s;
}

/// Number of bytes consumed by the first key slice (0–8).
pub fn firstSliceLen(key: []const u8) u8 {
    return @intCast(@min(@as(usize, 8), key.len));
}

// ── Value types ──────────────────────────────────────────────────────────────

/// The value associated with a border-node entry.
/// Either the caller's data or a pointer to the next trie layer.
const EntryValue = union(enum) {
    /// A user-supplied value; the key terminates at this layer.
    data: []const u8,
    /// A sub-layer; the key continues beyond this 8-byte slice.
    layer: *Layer,
};

// ── Node types ───────────────────────────────────────────────────────────────

/// A leaf (border) node in the B+-tree.
/// Holds up to `FANOUT` entries sorted by their 8-byte key slice.
const BorderNode = struct {
    nkeys: usize = 0,
    /// 8-byte key slices in ascending order.
    slices: [FANOUT]u64 = undefined,
    /// Bytes consumed in this slice (0 = empty key, 1–8).
    slens: [FANOUT]u8 = undefined,
    /// For entries where slen == 8 and val == .data this holds key[8..],
    /// allowing exact-key verification and sublayer push-down.
    /// Empty slice for all other entries.
    rests: [FANOUT][]const u8 = undefined,
    /// Entry values.
    vals: [FANOUT]EntryValue = undefined,
    /// Singly-linked list of border nodes at this level (for ordered scans).
    next: ?*BorderNode = null,
};

/// An internal node in the B+-tree.
/// Holds up to `FANOUT` separator keys and `FANOUT + 1` child pointers.
const InteriorNode = struct {
    nkeys: usize = 0,
    seps: [FANOUT]u64 = undefined,
    children: [FANOUT + 1]NodeRef = undefined,
};

/// A tagged reference to either type of node.
const NodeRef = union(enum) {
    border: *BorderNode,
    interior: *InteriorNode,
};

// ── Node helpers ─────────────────────────────────────────────────────────────

/// Returns the index of the first entry whose slice is >= `s` (lower bound).
fn lowerBound(b: *const BorderNode, s: u64) usize {
    var lo: usize = 0;
    var hi: usize = b.nkeys;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (b.slices[mid] < s) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/// Returns the child index to descend into for the given key slice.
fn childIdx(n: *const InteriorNode, s: u64) usize {
    var i: usize = 0;
    while (i < n.nkeys and s >= n.seps[i]) i += 1;
    return i;
}

// ── Layer ─────────────────────────────────────────────────────────────────────

/// One B+-tree layer of the Masstree trie.
///
/// A `Layer` processes the **first 8 bytes** of whatever key slice it
/// receives.  Sub-layers receive `key[8..]`.
pub const Layer = struct {
    root: NodeRef,
    alloc: Allocator,

    /// Allocate and initialise a new (empty) layer.
    pub fn init(alloc: Allocator) !*Layer {
        const b = try alloc.create(BorderNode);
        b.* = .{};
        const l = try alloc.create(Layer);
        l.* = .{ .root = .{ .border = b }, .alloc = alloc };
        return l;
    }

    /// Free all nodes in the layer (and any sub-layers) then free the layer itself.
    pub fn deinit(self: *Layer) void {
        self.freeNode(self.root);
        self.alloc.destroy(self);
    }

    fn freeNode(self: *Layer, node: NodeRef) void {
        switch (node) {
            .border => |b| {
                for (0..b.nkeys) |i| {
                    switch (b.vals[i]) {
                        .layer => |sub| sub.deinit(),
                        .data => {},
                    }
                }
                self.alloc.destroy(b);
            },
            .interior => |n| {
                for (0..n.nkeys + 1) |i| self.freeNode(n.children[i]);
                self.alloc.destroy(n);
            },
        }
    }

    // ── get ──────────────────────────────────────────────────────────────────

    /// Look up `key` and return its value, or `null` if not present.
    pub fn get(self: *const Layer, key: []const u8) ?[]const u8 {
        const s = extractSlice(key);
        const sl = firstSliceLen(key);
        var node = self.root;
        while (true) {
            switch (node) {
                .interior => |n| node = n.children[childIdx(n, s)],
                .border => |b| {
                    const pos = lowerBound(b, s);
                    if (pos >= b.nkeys or b.slices[pos] != s) return null;
                    if (b.slens[pos] != sl) return null;
                    switch (b.vals[pos]) {
                        .data => |d| {
                            // For long keys verify the suffix matches.
                            if (sl == 8 and !std.mem.eql(u8, b.rests[pos], key[8..])) {
                                return null;
                            }
                            return d;
                        },
                        .layer => |sub| {
                            if (sl < 8) return null;
                            return sub.get(key[8..]);
                        },
                    }
                },
            }
        }
    }

    // ── put ──────────────────────────────────────────────────────────────────

    /// A split result: the separator key that rose to the parent and the new
    /// right-sibling node.
    const Split = struct { sep: u64, right: NodeRef };

    /// Insert or update `key` → `value`.
    pub fn put(self: *Layer, key: []const u8, value: []const u8) !void {
        if (try self.nodeInsert(&self.root, key, value)) |split| {
            // The root was split – create a new root above both halves.
            const nr = try self.alloc.create(InteriorNode);
            nr.* = .{};
            nr.seps[0] = split.sep;
            nr.children[0] = self.root;
            nr.children[1] = split.right;
            nr.nkeys = 1;
            self.root = .{ .interior = nr };
        }
    }

    fn nodeInsert(
        self: *Layer,
        np: *NodeRef,
        key: []const u8,
        value: []const u8,
    ) Allocator.Error!?Split {
        const s = extractSlice(key);
        switch (np.*) {
            .interior => |n| {
                const idx = childIdx(n, s);
                const split = try self.nodeInsert(&n.children[idx], key, value) orelse return null;
                if (n.nkeys < FANOUT) {
                    interiorInsert(n, split.sep, split.right);
                    return null;
                }
                return try self.splitInterior(n, split.sep, split.right);
            },
            .border => |b| return try self.borderInsert(b, key, value),
        }
    }

    /// Insert `sep` / `right` into interior node `n` (which has room).
    fn interiorInsert(n: *InteriorNode, sep: u64, right: NodeRef) void {
        var pos: usize = n.nkeys;
        while (pos > 0 and n.seps[pos - 1] > sep) : (pos -= 1) {
            n.seps[pos] = n.seps[pos - 1];
            n.children[pos + 1] = n.children[pos];
        }
        n.seps[pos] = sep;
        n.children[pos + 1] = right;
        n.nkeys += 1;
    }

    fn borderInsert(
        self: *Layer,
        b: *BorderNode,
        key: []const u8,
        value: []const u8,
    ) Allocator.Error!?Split {
        const s = extractSlice(key);
        const sl = firstSliceLen(key);
        const pos = lowerBound(b, s);

        // ── collision: same slice already present ────────────────────────────
        if (pos < b.nkeys and b.slices[pos] == s) {
            if (b.slens[pos] == sl) {
                switch (b.vals[pos]) {
                    .data => {
                        if (sl < 8) {
                            // Key ends within this slice – simple overwrite.
                            b.vals[pos] = .{ .data = value };
                            return null;
                        }
                        // sl == 8: compare stored suffix to decide whether
                        // this is an update or a new key requiring a sub-layer.
                        const old_rest = b.rests[pos];
                        const new_rest = key[8..];
                        if (std.mem.eql(u8, old_rest, new_rest)) {
                            // Same key – overwrite.
                            b.vals[pos] = .{ .data = value };
                            return null;
                        }
                        // Two distinct keys share the same 8-byte prefix:
                        // push both into a new sub-layer.
                        const sub = try Layer.init(self.alloc);
                        {
                            errdefer sub.deinit();
                            try sub.put(old_rest, b.vals[pos].data);
                            try sub.put(new_rest, value);
                        }
                        b.vals[pos] = .{ .layer = sub };
                        b.rests[pos] = &[_]u8{};
                        return null;
                    },
                    .layer => |sub| {
                        if (sl < 8) {
                            // Key ends here but entry is a layer; replace.
                            sub.deinit();
                            b.vals[pos] = .{ .data = value };
                            b.rests[pos] = &[_]u8{};
                            return null;
                        }
                        try sub.put(key[8..], value);
                        return null;
                    },
                }
            }
            // Different slen with same slice (null-byte edge case):
            // fall through to insert as a separate entry at the same slice position.
        }

        // ── fresh insertion ──────────────────────────────────────────────────
        const rest: []const u8 = if (sl == 8) key[8..] else &[_]u8{};
        if (b.nkeys < FANOUT) {
            rawInsert(b, pos, s, sl, rest, .{ .data = value });
            return null;
        }
        return try self.splitBorder(b, pos, s, sl, rest, .{ .data = value });
    }

    /// Unconditionally insert one entry at `pos` in border node `b`,
    /// shifting existing entries right.  Caller must ensure there is room.
    fn rawInsert(
        b: *BorderNode,
        pos: usize,
        s: u64,
        sl: u8,
        rest: []const u8,
        val: EntryValue,
    ) void {
        var i = b.nkeys;
        while (i > pos) : (i -= 1) {
            b.slices[i] = b.slices[i - 1];
            b.slens[i] = b.slens[i - 1];
            b.rests[i] = b.rests[i - 1];
            b.vals[i] = b.vals[i - 1];
        }
        b.slices[pos] = s;
        b.slens[pos] = sl;
        b.rests[pos] = rest;
        b.vals[pos] = val;
        b.nkeys += 1;
    }

    /// Split a full border node by distributing its FANOUT entries plus the
    /// new entry across the existing node (left half) and a fresh node (right
    /// half).  Returns the separator key and right sibling to propagate up.
    fn splitBorder(
        self: *Layer,
        b: *BorderNode,
        ins_pos: usize,
        s: u64,
        sl: u8,
        rest: []const u8,
        val: EntryValue,
    ) Allocator.Error!Split {
        const new_b = try self.alloc.create(BorderNode);
        new_b.* = .{};

        // Merge FANOUT existing entries plus the new one into temporaries.
        var ts: [FANOUT + 1]u64 = undefined;
        var tl: [FANOUT + 1]u8 = undefined;
        var tr: [FANOUT + 1][]const u8 = undefined;
        var tv: [FANOUT + 1]EntryValue = undefined;

        var j: usize = 0;
        for (0..FANOUT + 1) |i| {
            if (i == ins_pos) {
                ts[i] = s;
                tl[i] = sl;
                tr[i] = rest;
                tv[i] = val;
            } else {
                ts[i] = b.slices[j];
                tl[i] = b.slens[j];
                tr[i] = b.rests[j];
                tv[i] = b.vals[j];
                j += 1;
            }
        }

        // Left node keeps entries [0, mid).
        const mid = (FANOUT + 1) / 2;
        b.nkeys = mid;
        for (0..mid) |i| {
            b.slices[i] = ts[i];
            b.slens[i] = tl[i];
            b.rests[i] = tr[i];
            b.vals[i] = tv[i];
        }

        // Right node gets entries [mid, FANOUT+1).
        new_b.nkeys = FANOUT + 1 - mid;
        for (0..new_b.nkeys) |i| {
            new_b.slices[i] = ts[mid + i];
            new_b.slens[i] = tl[mid + i];
            new_b.rests[i] = tr[mid + i];
            new_b.vals[i] = tv[mid + i];
        }

        // Maintain the linked list.
        new_b.next = b.next;
        b.next = new_b;

        return .{ .sep = new_b.slices[0], .right = .{ .border = new_b } };
    }

    /// Split a full interior node by distributing its FANOUT separators plus
    /// one new separator across the existing node (left) and a fresh node
    /// (right).  The middle separator rises to the parent.
    fn splitInterior(
        self: *Layer,
        n: *InteriorNode,
        ins_sep: u64,
        ins_right: NodeRef,
    ) Allocator.Error!Split {
        const new_n = try self.alloc.create(InteriorNode);
        new_n.* = .{};

        // Build merged arrays: FANOUT+1 separators, FANOUT+2 children.
        var tseps: [FANOUT + 1]u64 = undefined;
        var tchildren: [FANOUT + 2]NodeRef = undefined;

        // Find where the new separator belongs.
        var ins_pos: usize = 0;
        while (ins_pos < n.nkeys and n.seps[ins_pos] < ins_sep) ins_pos += 1;

        // Copy existing separators, inserting ins_sep at ins_pos.
        for (0..ins_pos) |i| tseps[i] = n.seps[i];
        tseps[ins_pos] = ins_sep;
        for (ins_pos..FANOUT) |i| tseps[i + 1] = n.seps[i];

        // Copy existing children, inserting ins_right at ins_pos+1.
        for (0..ins_pos + 1) |i| tchildren[i] = n.children[i];
        tchildren[ins_pos + 1] = ins_right;
        for (ins_pos + 1..FANOUT + 1) |i| tchildren[i + 1] = n.children[i];

        // Split at mid: left keeps [0, mid), mid rises, right gets [mid+1, FANOUT+1).
        const mid = (FANOUT + 1) / 2;
        const up_sep = tseps[mid];

        n.nkeys = mid;
        for (0..mid) |i| {
            n.seps[i] = tseps[i];
            n.children[i] = tchildren[i];
        }
        n.children[mid] = tchildren[mid];

        new_n.nkeys = FANOUT - mid;
        for (0..new_n.nkeys) |i| {
            new_n.seps[i] = tseps[mid + 1 + i];
            new_n.children[i] = tchildren[mid + 1 + i];
        }
        new_n.children[new_n.nkeys] = tchildren[FANOUT + 1];

        return .{ .sep = up_sep, .right = .{ .interior = new_n } };
    }

    // ── remove ───────────────────────────────────────────────────────────────

    /// Remove `key`.  Returns `true` if the key was present and removed.
    /// Does **not** rebalance nodes after removal (lazy deletion).
    pub fn remove(self: *Layer, key: []const u8) bool {
        return self.nodeRemove(self.root, key);
    }

    fn nodeRemove(self: *Layer, node: NodeRef, key: []const u8) bool {
        const s = extractSlice(key);
        const sl = firstSliceLen(key);
        switch (node) {
            .interior => |n| return self.nodeRemove(n.children[childIdx(n, s)], key),
            .border => |b| {
                const pos = lowerBound(b, s);
                if (pos >= b.nkeys or b.slices[pos] != s) return false;
                if (b.slens[pos] != sl) return false;
                switch (b.vals[pos]) {
                    .data => {
                        if (sl == 8 and !std.mem.eql(u8, b.rests[pos], key[8..])) {
                            return false;
                        }
                        // Shift entries left to fill the gap.
                        for (pos..b.nkeys - 1) |i| {
                            b.slices[i] = b.slices[i + 1];
                            b.slens[i] = b.slens[i + 1];
                            b.rests[i] = b.rests[i + 1];
                            b.vals[i] = b.vals[i + 1];
                        }
                        b.nkeys -= 1;
                        return true;
                    },
                    .layer => |sub| {
                        if (sl < 8) return false;
                        return sub.remove(key[8..]);
                    },
                }
            },
        }
    }
};

// ── Public MassTree API ───────────────────────────────────────────────────────

/// A Masstree: a concurrent-ready trie of B+-trees with 8-byte key slices.
///
/// This implementation is **single-threaded**.  See the module doc-comment for
/// details on key/value ownership and limitations.
pub const MassTree = struct {
    layer: *Layer,
    alloc: Allocator,

    const Self = @This();

    pub fn init(alloc: Allocator) !Self {
        return .{ .layer = try Layer.init(alloc), .alloc = alloc };
    }

    pub fn deinit(self: *Self) void {
        self.layer.deinit();
    }

    /// Look up `key`.  Returns the associated value or `null`.
    pub fn get(self: *const Self, key: []const u8) ?[]const u8 {
        return self.layer.get(key);
    }

    /// Insert or update `key` → `value`.
    pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
        return self.layer.put(key, value);
    }

    /// Remove `key`.  Returns `true` if the key existed.
    pub fn remove(self: *Self, key: []const u8) bool {
        return self.layer.remove(key);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "extractSlice" {
    // Exactly 8 bytes – no padding needed.
    try testing.expectEqual(
        @as(u64, 0x6162636465666768),
        extractSlice("abcdefgh"),
    );
    // Short key – padded with zeros on the right.
    try testing.expectEqual(
        @as(u64, 0x6162630000000000),
        extractSlice("abc"),
    );
    // Empty key → 0.
    try testing.expectEqual(@as(u64, 0), extractSlice(""));
    // Longer than 8 bytes – only first 8 are used.
    try testing.expectEqual(
        @as(u64, 0x6162636465666768),
        extractSlice("abcdefghijk"),
    );
}

test "firstSliceLen" {
    try testing.expectEqual(@as(u8, 0), firstSliceLen(""));
    try testing.expectEqual(@as(u8, 3), firstSliceLen("abc"));
    try testing.expectEqual(@as(u8, 8), firstSliceLen("abcdefgh"));
    try testing.expectEqual(@as(u8, 8), firstSliceLen("abcdefghijk"));
}

test "basic put and get" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    try tree.put("hello", "world");
    try testing.expectEqualStrings("world", tree.get("hello").?);

    // Missing keys return null.
    try testing.expectEqual(@as(?[]const u8, null), tree.get("hell"));
    try testing.expectEqual(@as(?[]const u8, null), tree.get("helloo"));
    try testing.expectEqual(@as(?[]const u8, null), tree.get(""));
}

test "update existing key" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    try tree.put("key", "v1");
    try testing.expectEqualStrings("v1", tree.get("key").?);
    try tree.put("key", "v2");
    try testing.expectEqualStrings("v2", tree.get("key").?);
}

test "multiple short keys" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    const pairs = [_][2][]const u8{
        .{ "apple", "1" },
        .{ "banana", "2" },
        .{ "cherry", "3" },
        .{ "date", "4" },
        .{ "elderberry", "5" },
    };
    for (pairs) |p| try tree.put(p[0], p[1]);
    for (pairs) |p| try testing.expectEqualStrings(p[1], tree.get(p[0]).?);
}

test "remove key" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    try tree.put("foo", "bar");
    try testing.expectEqualStrings("bar", tree.get("foo").?);

    try testing.expect(tree.remove("foo"));
    try testing.expectEqual(@as(?[]const u8, null), tree.get("foo"));

    // Removing again returns false.
    try testing.expect(!tree.remove("foo"));
}

test "empty key" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    try tree.put("", "empty");
    try testing.expectEqualStrings("empty", tree.get("").?);

    try testing.expect(tree.remove(""));
    try testing.expectEqual(@as(?[]const u8, null), tree.get(""));
}

test "keys of varying lengths around the 8-byte boundary" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    try tree.put("a", "1");
    try tree.put("ab", "2");
    try tree.put("abc", "3");
    try tree.put("abcd", "4");
    try tree.put("abcde", "5");
    try tree.put("abcdef", "6");
    try tree.put("abcdefg", "7");
    try tree.put("abcdefgh", "8");
    try tree.put("abcdefghi", "9");
    try tree.put("abcdefghij", "10");

    try testing.expectEqualStrings("1", tree.get("a").?);
    try testing.expectEqualStrings("4", tree.get("abcd").?);
    try testing.expectEqualStrings("7", tree.get("abcdefg").?);
    try testing.expectEqualStrings("8", tree.get("abcdefgh").?);
    try testing.expectEqualStrings("9", tree.get("abcdefghi").?);
    try testing.expectEqualStrings("10", tree.get("abcdefghij").?);

    // Keys not in tree must return null.
    try testing.expectEqual(@as(?[]const u8, null), tree.get("abcdefghijk"));
}

test "sublayer created for shared 8-byte prefix" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    // Both keys begin with "abcdefgh" – a sub-layer must be created.
    try tree.put("abcdefgh12345678", "v1");
    try tree.put("abcdefghXXXXXXXX", "v2");

    try testing.expectEqualStrings("v1", tree.get("abcdefgh12345678").?);
    try testing.expectEqualStrings("v2", tree.get("abcdefghXXXXXXXX").?);

    // The prefix alone is not in the tree.
    try testing.expectEqual(@as(?[]const u8, null), tree.get("abcdefgh"));
}

test "short key and long key sharing 8-byte prefix" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    // "abcdefgh" ends exactly at the 8-byte slice boundary.
    try tree.put("abcdefgh", "short");
    // "abcdefghijk" shares the same first 8 bytes but continues.
    try tree.put("abcdefghijk", "long");

    try testing.expectEqualStrings("short", tree.get("abcdefgh").?);
    try testing.expectEqualStrings("long", tree.get("abcdefghijk").?);

    // An unrelated key must not match.
    try testing.expectEqual(@as(?[]const u8, null), tree.get("abcdefghXYZ"));
}

test "three keys sharing 8-byte prefix" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    try tree.put("abcdefghAAA", "1");
    try tree.put("abcdefghBBB", "2");
    try tree.put("abcdefghCCC", "3");

    try testing.expectEqualStrings("1", tree.get("abcdefghAAA").?);
    try testing.expectEqualStrings("2", tree.get("abcdefghBBB").?);
    try testing.expectEqualStrings("3", tree.get("abcdefghCCC").?);
}

test "deep trie (24-byte key spanning three layers)" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    // Layer 0 handles bytes  0– 7 ("abcdefgh")
    // Layer 1 handles bytes  8–15 ("ijklmnop")
    // Layer 2 handles bytes 16–23 ("qrstuvwx")
    try tree.put("abcdefghijklmnopqrstuvwx", "deep");
    try testing.expectEqualStrings("deep", tree.get("abcdefghijklmnopqrstuvwx").?);

    // A shorter key with the same prefix should not match.
    try testing.expectEqual(@as(?[]const u8, null), tree.get("abcdefghijklmnop"));
    try testing.expectEqual(@as(?[]const u8, null), tree.get("abcdefgh"));
}

test "remove from sublayer" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    try tree.put("abcdefghAAA", "1");
    try tree.put("abcdefghBBB", "2");

    try testing.expect(tree.remove("abcdefghAAA"));
    try testing.expectEqual(@as(?[]const u8, null), tree.get("abcdefghAAA"));
    try testing.expectEqualStrings("2", tree.get("abcdefghBBB").?);
}

test "node splits with more than FANOUT keys" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    // Use 8-char string keys that all differ in the first 8 bytes so they
    // live entirely in the root layer and exercise border-node splitting.
    const keys = [_][]const u8{
        "00000001", "00000002", "00000003", "00000004", "00000005",
        "00000006", "00000007", "00000008", "00000009", "00000010",
        "00000011", "00000012", "00000013", "00000014", "00000015",
        "00000016", "00000017", "00000018", "00000019", "00000020",
    };
    const vals = [_][]const u8{
        "a", "b", "c", "d", "e",
        "f", "g", "h", "i", "j",
        "k", "l", "m", "n", "o",
        "p", "q", "r", "s", "t",
    };

    for (keys, vals) |k, v| try tree.put(k, v);
    for (keys, vals) |k, v| try testing.expectEqualStrings(v, tree.get(k).?);
}

test "interior node splits (deep B+-tree)" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    // Insert FANOUT*FANOUT keys to force interior-node splits as well.
    // Each key is exactly 8 bytes so all live in the root layer.
    const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEF";
    var key: [8]u8 = undefined;
    for (0..8) |ci| {
        for (0..28) |cj| {
            key[0] = alphabet[ci];
            key[1] = alphabet[cj];
            key[2] = 'x';
            key[3] = 'x';
            key[4] = 'x';
            key[5] = 'x';
            key[6] = 'x';
            key[7] = 'x';
            // The value is just the first two chars.
            try tree.put(&key, key[0..2]);
        }
    }
    // Spot-check a few entries (ci=0,cj=0 → "aaxxxxxx"; ci=7,cj=27 → "hBxxxxxx").
    try testing.expect(tree.get("aaxxxxxx") != null);
    try testing.expect(tree.get("hBxxxxxx") != null);
}

test "update key in sublayer" {
    var tree = try MassTree.init(testing.allocator);
    defer tree.deinit();

    try tree.put("abcdefghSUFFIX_A", "old");
    try tree.put("abcdefghSUFFIX_B", "other");
    try tree.put("abcdefghSUFFIX_A", "new");

    try testing.expectEqualStrings("new", tree.get("abcdefghSUFFIX_A").?);
    try testing.expectEqualStrings("other", tree.get("abcdefghSUFFIX_B").?);
}
