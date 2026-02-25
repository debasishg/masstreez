//! Range scan support for MassTree.
//!
//! Provides forward and reverse iteration over key-value pairs within
//! a specified key range. This is the single-threaded implementation;
//! Phase 4 will add OCC-based concurrent range scans.
//!
//! ## Design
//!
//! The iterator uses a layer stack to track position across trie layers.
//! When a layer pointer is encountered, the current context is pushed
//! onto the stack and scanning descends into the sublayer. When a
//! sublayer is exhausted, the context is popped and scanning resumes
//! in the parent.
//!
//! Key reconstruction uses a `CursorKey` that accumulates prefix bytes
//! as we descend through layers, then builds the full key for each
//! emitted entry.
//!
//! Start/end bound checking uses a flag-based approach: after navigating
//! to the approximate start position, a `start_active` flag causes
//! entries before the start bound to be skipped until the first valid
//! entry is found. The end bound is checked on every emitted entry.

const std = @import("std");

const key_mod = @import("key.zig");
const Key = key_mod.Key;
const IKEY_SIZE = key_mod.IKEY_SIZE;
const MAX_KEY_LENGTH = key_mod.MAX_KEY_LENGTH;
const KSUF_KEYLENX = key_mod.KSUF_KEYLENX;
const LAYER_KEYLENX = key_mod.LAYER_KEYLENX;

const leaf_mod = @import("leaf.zig");
const interior_mod = @import("interior.zig");
const InternodeNode = interior_mod.InternodeNode;
const value_mod = @import("value.zig");

/// Maximum trie layer depth (256 / 8 = 32).
const MAX_DEPTH: usize = MAX_KEY_LENGTH / IKEY_SIZE;

// ============================================================================
//  RangeBound
// ============================================================================

/// Specifies the start or end of a key range.
pub const RangeBound = union(enum) {
    /// No bound (scan from beginning / to end).
    unbounded: void,
    /// Bound includes the specified key.
    included: []const u8,
    /// Bound excludes the specified key.
    excluded: []const u8,

    /// Check if `key_bytes` is within this bound when used as an end bound.
    /// Returns true if key <= included bound, or key < excluded bound.
    pub fn contains_end(self: RangeBound, key_bytes: []const u8) bool {
        return switch (self) {
            .unbounded => true,
            .included => |bound| std.mem.order(u8, key_bytes, bound) != .gt,
            .excluded => |bound| std.mem.order(u8, key_bytes, bound) == .lt,
        };
    }

    /// Check if `key_bytes` is within this bound when used as a start bound.
    /// Returns true if key >= included bound, or key > excluded bound.
    pub fn contains_start(self: RangeBound, key_bytes: []const u8) bool {
        return switch (self) {
            .unbounded => true,
            .included => |bound| std.mem.order(u8, key_bytes, bound) != .lt,
            .excluded => |bound| std.mem.order(u8, key_bytes, bound) == .gt,
        };
    }
};

// ============================================================================
//  CursorKey
// ============================================================================

/// Mutable cursor for tracking position during range scan.
///
/// Accumulates key prefix bytes as we descend through trie layers,
/// then builds the full key for each emitted entry from the prefix
/// plus the current slot's ikey and optional suffix.
const CursorKey = struct {
    /// Buffer holding accumulated key bytes from ancestor layers.
    buf: [MAX_KEY_LENGTH]u8 = [_]u8{0} ** MAX_KEY_LENGTH,
    /// Current trie layer depth (0 = root layer).
    depth: usize = 0,

    /// Descend into a sublayer: record the layer's ikey and bump depth.
    fn shift(self: *CursorKey, ik: u64) void {
        const off = self.depth * IKEY_SIZE;
        std.mem.writeInt(u64, self.buf[off..][0..IKEY_SIZE], ik, .big);
        self.depth += 1;
    }

    /// Ascend to parent layer.
    fn unshift(self: *CursorKey) void {
        std.debug.assert(self.depth > 0);
        self.depth -= 1;
    }

    /// Build the full key for a leaf slot into the provided output buffer.
    ///
    /// Returns a slice of `out` containing the reconstructed key bytes.
    /// The prefix comes from ancestor layers stored in `self.buf`,
    /// the current 8-byte ikey is appended, and the suffix (if any)
    /// is appended after that.
    fn build_key(
        self: *const CursorKey,
        out: []u8,
        ik: u64,
        klx: u8,
        suf: ?[]const u8,
    ) []const u8 {
        const prefix_len = self.depth * IKEY_SIZE;

        // Copy prefix from ancestor layers
        if (prefix_len > 0) {
            @memcpy(out[0..prefix_len], self.buf[0..prefix_len]);
        }

        // Write current ikey bytes
        const ikey_bytes = key_mod.make_slice(ik);

        if (klx == KSUF_KEYLENX) {
            // Key = prefix + 8 bytes ikey + suffix
            @memcpy(out[prefix_len..][0..IKEY_SIZE], &ikey_bytes);
            const s = suf orelse &[_]u8{};
            if (s.len > 0) {
                @memcpy(out[prefix_len + IKEY_SIZE ..][0..s.len], s);
            }
            return out[0 .. prefix_len + IKEY_SIZE + s.len];
        } else if (klx <= 8) {
            // Key = prefix + klx bytes of ikey (inline key)
            const kl: usize = @intCast(klx);
            @memcpy(out[prefix_len..][0..kl], ikey_bytes[0..kl]);
            return out[0 .. prefix_len + kl];
        } else {
            // Fallback (shouldn't happen for value slots)
            @memcpy(out[prefix_len..][0..IKEY_SIZE], &ikey_bytes);
            return out[0 .. prefix_len + IKEY_SIZE];
        }
    }
};

// ============================================================================
//  Navigation helpers
// ============================================================================

/// Navigate from a root pointer to the leftmost leaf.
fn navigate_to_leftmost_leaf(comptime Leaf: type, root: *anyopaque, root_is_leaf: bool) ?*const Leaf {
    if (root_is_leaf) {
        return @ptrCast(@alignCast(root));
    }

    var inode: *const InternodeNode = @ptrCast(@alignCast(root));
    while (true) {
        const child = inode.children[0] orelse return null;
        if (inode.height == 0) {
            return @ptrCast(@alignCast(child));
        }
        inode = @ptrCast(@alignCast(child));
    }
}

/// Navigate from a root pointer to the rightmost leaf.
fn navigate_to_rightmost_leaf(comptime Leaf: type, root: *anyopaque, root_is_leaf: bool) ?*const Leaf {
    if (root_is_leaf) {
        return @ptrCast(@alignCast(root));
    }

    var inode: *const InternodeNode = @ptrCast(@alignCast(root));
    while (true) {
        const idx: usize = @as(usize, inode.nkeys);
        const child = inode.children[idx] orelse return null;
        if (inode.height == 0) {
            return @ptrCast(@alignCast(child));
        }
        inode = @ptrCast(@alignCast(child));
    }
}

/// Navigate from a root to the leaf that would contain the given ikey.
fn navigate_to_leaf(comptime Leaf: type, root: *anyopaque, root_is_leaf: bool, search_ikey: u64) ?*const Leaf {
    if (root_is_leaf) {
        return @ptrCast(@alignCast(root));
    }

    var inode: *const InternodeNode = @ptrCast(@alignCast(root));
    while (true) {
        const child_idx = inode.upper_bound(search_ikey);
        const child = inode.get_child(child_idx) orelse return null;
        if (inode.height == 0) {
            return @ptrCast(@alignCast(child));
        }
        inode = @ptrCast(@alignCast(child));
    }
}

// ============================================================================
//  RangeIterator(V) — forward iteration
// ============================================================================

/// Forward range iterator over a MassTree.
///
/// Yields entries in ascending key order within the specified range.
/// Entry keys are slices into an internal buffer and are only valid
/// until the next call to `next()`.
pub fn RangeIterator(comptime V: type) type {
    const Leaf = leaf_mod.LeafNode(V);

    return struct {
        const Self = @This();

        /// A key-value entry yielded by the iterator.
        ///
        /// The `key` slice points into the iterator's internal buffer
        /// and is only valid until the next call to `next()`.
        pub const Entry = struct {
            key: []const u8,
            value: V,
        };

        /// Saved context when descending into a sublayer.
        const LayerCtx = struct {
            leaf: *const Leaf,
            ki: usize,
        };

        // --- Current scan position ---
        /// Current leaf being scanned (null = layer exhausted).
        leaf: ?*const Leaf,
        /// Next logical position in the permutation to examine.
        ki: usize,

        // --- Layer stack ---
        stack: [MAX_DEPTH]LayerCtx,
        stack_depth: usize,

        // --- Key reconstruction ---
        cursor: CursorKey,
        key_buf: [MAX_KEY_LENGTH]u8,

        // --- Bounds ---
        start_bound: RangeBound,
        end_bound: RangeBound,

        // --- State ---
        /// True until the first entry >= start bound is found.
        start_active: bool,
        /// True when the iterator is exhausted.
        exhausted: bool,

        // --- Tree root (for sublayer navigation) ---
        tree_root: *anyopaque,
        tree_root_is_leaf: bool,

        /// Create a forward range iterator.
        pub fn init(
            root: *anyopaque,
            root_is_leaf: bool,
            start_bound: RangeBound,
            end_bound: RangeBound,
        ) Self {
            var self = Self{
                .leaf = null,
                .ki = 0,
                .stack = undefined,
                .stack_depth = 0,
                .cursor = .{},
                .key_buf = undefined,
                .start_bound = start_bound,
                .end_bound = end_bound,
                .start_active = false,
                .exhausted = false,
                .tree_root = root,
                .tree_root_is_leaf = root_is_leaf,
            };

            self.initialize(root, root_is_leaf);
            return self;
        }

        /// Initialize the iterator to the starting position.
        fn initialize(self: *Self, root: *anyopaque, root_is_leaf: bool) void {
            switch (self.start_bound) {
                .unbounded => {
                    self.leaf = navigate_to_leftmost_leaf(Leaf, root, root_is_leaf);
                    self.ki = 0;
                    self.start_active = false;
                },
                .included => |bound| {
                    self.find_start(bound, root, root_is_leaf);
                    self.start_active = true;
                },
                .excluded => |bound| {
                    self.find_start(bound, root, root_is_leaf);
                    self.start_active = true;
                },
            }
        }

        /// Navigate to the leaf containing the start key's ikey and
        /// set `ki` to the approximate starting position.
        fn find_start(self: *Self, start_key: []const u8, root: *anyopaque, root_is_leaf: bool) void {
            const k = Key.init(start_key);
            const leaf = navigate_to_leaf(Leaf, root, root_is_leaf, k.ikey()) orelse {
                self.leaf = null;
                return;
            };

            // Use lower_bound to find approximate position
            const lb = leaf.lower_bound(k.ikey());
            self.leaf = leaf;
            self.ki = lb;
        }

        /// Get the next entry in ascending key order, or null if done.
        pub fn next(self: *Self) ?Entry {
            while (!self.exhausted) {
                const leaf = self.leaf orelse {
                    // Current layer exhausted — try ascending
                    if (!self.ascend()) {
                        self.exhausted = true;
                        return null;
                    }
                    continue;
                };

                const perm = leaf.permutation;
                const size = perm.size();

                // Exhausted this leaf — advance to sibling
                if (self.ki >= size) {
                    if (leaf.next) |next_leaf| {
                        self.leaf = next_leaf;
                        self.ki = 0;
                    } else {
                        self.leaf = null;
                    }
                    continue;
                }

                const slot = perm.get(self.ki);
                self.ki += 1;

                const slot_ikey = leaf.ikeys[slot];
                const slot_klx = leaf.keylenx[slot];

                // Skip empty slots
                if (leaf.values[slot].is_empty()) continue;

                // Layer pointer — descend into sublayer
                if (slot_klx >= LAYER_KEYLENX) {
                    if (leaf.values[slot].try_as_layer()) |ptr| {
                        self.descend(leaf, slot_ikey, ptr);
                    }
                    continue;
                }

                // Build full key for this entry
                const suf: ?[]const u8 = if (slot_klx == KSUF_KEYLENX)
                    leaf.ksuf(slot)
                else
                    null;

                const key = self.cursor.build_key(&self.key_buf, slot_ikey, slot_klx, suf);

                // Check start bound (skip entries before start)
                if (self.start_active) {
                    if (!self.start_bound.contains_start(key)) continue;
                    self.start_active = false;
                }

                // Check end bound
                if (!self.end_bound.contains_end(key)) {
                    self.exhausted = true;
                    return null;
                }

                // Emit the entry
                const val = leaf.get_value(slot) orelse continue;
                return Entry{ .key = key, .value = val };
            }
            return null;
        }

        /// Descend into a sublayer.
        fn descend(self: *Self, parent_leaf: *const Leaf, layer_ikey: u64, layer_root: *anyopaque) void {
            // Save current context
            self.stack[self.stack_depth] = .{
                .leaf = parent_leaf,
                .ki = self.ki, // already incremented past the layer slot
            };
            self.stack_depth += 1;

            // Record layer ikey in cursor
            self.cursor.shift(layer_ikey);

            // Navigate to leftmost leaf in sublayer
            self.leaf = navigate_to_leftmost_leaf(Leaf, layer_root, true);
            self.ki = 0;
        }

        /// Ascend back to parent layer. Returns false if already at top.
        fn ascend(self: *Self) bool {
            if (self.stack_depth == 0) return false;

            self.stack_depth -= 1;
            const ctx = self.stack[self.stack_depth];

            self.cursor.unshift();
            self.leaf = ctx.leaf;
            self.ki = ctx.ki;

            return true;
        }
    };
}

// ============================================================================
//  ReverseRangeIterator(V) — backward iteration
// ============================================================================

/// Reverse range iterator over a MassTree.
///
/// Yields entries in descending key order within the specified range.
/// Entry keys are slices into an internal buffer and are only valid
/// until the next call to `next()`.
pub fn ReverseRangeIterator(comptime V: type) type {
    const Leaf = leaf_mod.LeafNode(V);

    return struct {
        const Self = @This();

        /// A key-value entry yielded by the iterator.
        pub const Entry = struct {
            key: []const u8,
            value: V,
        };

        /// Saved context when descending into a sublayer.
        const LayerCtx = struct {
            leaf: *const Leaf,
            ki: usize,
        };

        // --- Current scan position ---
        /// Current leaf being scanned (null = layer exhausted).
        leaf: ?*const Leaf,
        /// Count of remaining unexamined entries in current leaf.
        /// Starts at `size` or end position. Decremented before examining.
        /// When 0, the leaf is exhausted backward.
        ki: usize,

        // --- Layer stack ---
        stack: [MAX_DEPTH]LayerCtx,
        stack_depth: usize,

        // --- Key reconstruction ---
        cursor: CursorKey,
        key_buf: [MAX_KEY_LENGTH]u8,

        // --- Bounds ---
        /// Low bound — checked on every entry to know when to stop.
        start_bound: RangeBound,
        /// High bound — checked until first valid entry (end_active).
        end_bound: RangeBound,

        // --- State ---
        /// True until the first entry within the end bound is found.
        end_active: bool,
        /// True when the iterator is exhausted.
        exhausted: bool,

        // --- Tree root ---
        tree_root: *anyopaque,
        tree_root_is_leaf: bool,

        /// Create a reverse range iterator.
        pub fn init(
            root: *anyopaque,
            root_is_leaf: bool,
            start_bound: RangeBound,
            end_bound: RangeBound,
        ) Self {
            var self = Self{
                .leaf = null,
                .ki = 0,
                .stack = undefined,
                .stack_depth = 0,
                .cursor = .{},
                .key_buf = undefined,
                .start_bound = start_bound,
                .end_bound = end_bound,
                .end_active = false,
                .exhausted = false,
                .tree_root = root,
                .tree_root_is_leaf = root_is_leaf,
            };

            self.initialize(root, root_is_leaf);
            return self;
        }

        /// Initialize the iterator to the ending position.
        fn initialize(self: *Self, root: *anyopaque, root_is_leaf: bool) void {
            switch (self.end_bound) {
                .unbounded => {
                    const leaf = navigate_to_rightmost_leaf(Leaf, root, root_is_leaf);
                    self.leaf = leaf;
                    self.ki = if (leaf) |l| l.permutation.size() else 0;
                    self.end_active = false;
                },
                .included => |bound| {
                    self.find_end(bound, root, root_is_leaf);
                    self.end_active = true;
                },
                .excluded => |bound| {
                    self.find_end(bound, root, root_is_leaf);
                    self.end_active = true;
                },
            }
        }

        /// Navigate to the leaf containing the end key's ikey and
        /// set `ki` to scan from the end of that leaf backward.
        fn find_end(self: *Self, end_key: []const u8, root: *anyopaque, root_is_leaf: bool) void {
            const k = Key.init(end_key);
            const leaf = navigate_to_leaf(Leaf, root, root_is_leaf, k.ikey()) orelse {
                self.leaf = null;
                return;
            };

            self.leaf = leaf;
            // Start from the end of the leaf and let end_active skip
            // entries past the end bound.
            self.ki = leaf.permutation.size();
        }

        /// Get the next entry in descending key order, or null if done.
        pub fn next(self: *Self) ?Entry {
            while (!self.exhausted) {
                const leaf = self.leaf orelse {
                    // Current layer exhausted — try ascending
                    if (!self.ascend()) {
                        self.exhausted = true;
                        return null;
                    }
                    continue;
                };

                // Exhausted this leaf backward — retreat to prev sibling
                if (self.ki == 0) {
                    if (leaf.prev) |prev_leaf| {
                        self.leaf = prev_leaf;
                        self.ki = prev_leaf.permutation.size();
                    } else {
                        self.leaf = null;
                    }
                    continue;
                }

                self.ki -= 1;
                const perm = leaf.permutation;
                const slot = perm.get(self.ki);

                const slot_ikey = leaf.ikeys[slot];
                const slot_klx = leaf.keylenx[slot];

                // Skip empty slots
                if (leaf.values[slot].is_empty()) continue;

                // Layer pointer — descend into sublayer from the right
                if (slot_klx >= LAYER_KEYLENX) {
                    if (leaf.values[slot].try_as_layer()) |ptr| {
                        self.descend(leaf, slot_ikey, ptr);
                    }
                    continue;
                }

                // Build full key for this entry
                const suf: ?[]const u8 = if (slot_klx == KSUF_KEYLENX)
                    leaf.ksuf(slot)
                else
                    null;

                const key = self.cursor.build_key(&self.key_buf, slot_ikey, slot_klx, suf);

                // Check end bound (skip entries past the high end)
                if (self.end_active) {
                    if (!self.end_bound.contains_end(key)) continue;
                    self.end_active = false;
                }

                // Check start bound (stop at low end)
                if (!self.start_bound.contains_start(key)) {
                    self.exhausted = true;
                    return null;
                }

                // Emit the entry
                const val = leaf.get_value(slot) orelse continue;
                return Entry{ .key = key, .value = val };
            }
            return null;
        }

        /// Descend into a sublayer from the right (rightmost leaf, last position).
        fn descend(self: *Self, parent_leaf: *const Leaf, layer_ikey: u64, layer_root: *anyopaque) void {
            // Save current context (ki has already been decremented past the layer slot)
            self.stack[self.stack_depth] = .{
                .leaf = parent_leaf,
                .ki = self.ki,
            };
            self.stack_depth += 1;

            // Record layer ikey in cursor
            self.cursor.shift(layer_ikey);

            // Navigate to rightmost leaf in sublayer
            const leaf = navigate_to_rightmost_leaf(Leaf, layer_root, true);
            self.leaf = leaf;
            self.ki = if (leaf) |l| l.permutation.size() else 0;
        }

        /// Ascend back to parent layer. Returns false if already at top.
        fn ascend(self: *Self) bool {
            if (self.stack_depth == 0) return false;

            self.stack_depth -= 1;
            const ctx = self.stack[self.stack_depth];

            self.cursor.unshift();
            self.leaf = ctx.leaf;
            self.ki = ctx.ki;

            return true;
        }
    };
}

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "CursorKey: build_key inline" {
    var cursor = CursorKey{};
    var buf: [MAX_KEY_LENGTH]u8 = undefined;

    // 5-byte key at depth 0
    const ik = key_mod.read_ikey("hello", 0);
    const key = cursor.build_key(&buf, ik, 5, null);
    try testing.expectEqualSlices(u8, "hello", key);
}

test "CursorKey: build_key with suffix" {
    var cursor = CursorKey{};
    var buf: [MAX_KEY_LENGTH]u8 = undefined;

    const ik = key_mod.read_ikey("AAAAAAAA", 0);
    const key = cursor.build_key(&buf, ik, KSUF_KEYLENX, "xyz");
    try testing.expectEqualSlices(u8, "AAAAAAAAxyz", key);
}

test "CursorKey: build_key with depth" {
    var cursor = CursorKey{};
    var buf: [MAX_KEY_LENGTH]u8 = undefined;

    // Simulate descending into layer with ikey "AAAAAAAA"
    const parent_ik = key_mod.read_ikey("AAAAAAAA", 0);
    cursor.shift(parent_ik);

    // Build key at depth 1 with 3-byte inline key
    const child_ik = key_mod.read_ikey("xyz", 0);
    const key = cursor.build_key(&buf, child_ik, 3, null);
    try testing.expectEqualSlices(u8, "AAAAAAAAxyz", key);
}

test "RangeBound: contains_end" {
    const ub = RangeBound{ .unbounded = {} };
    try testing.expect(ub.contains_end("anything"));

    const inc = RangeBound{ .included = "mmm" };
    try testing.expect(inc.contains_end("aaa"));
    try testing.expect(inc.contains_end("mmm"));
    try testing.expect(!inc.contains_end("zzz"));

    const exc = RangeBound{ .excluded = "mmm" };
    try testing.expect(exc.contains_end("aaa"));
    try testing.expect(!exc.contains_end("mmm"));
    try testing.expect(!exc.contains_end("zzz"));
}

test "RangeBound: contains_start" {
    const ub = RangeBound{ .unbounded = {} };
    try testing.expect(ub.contains_start("anything"));

    const inc = RangeBound{ .included = "mmm" };
    try testing.expect(!inc.contains_start("aaa"));
    try testing.expect(inc.contains_start("mmm"));
    try testing.expect(inc.contains_start("zzz"));

    const exc = RangeBound{ .excluded = "mmm" };
    try testing.expect(!exc.contains_start("aaa"));
    try testing.expect(!exc.contains_start("mmm"));
    try testing.expect(exc.contains_start("zzz"));
}
