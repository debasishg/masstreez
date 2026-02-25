//! A single **trie layer** — one complete B⁺ tree keyed on 8-byte
//! slices extracted from the full key at a given depth.
//!
//! ## Responsibilities
//!
//! * **Lookup** — descend interior nodes to the correct leaf, then
//!   linear-scan the leaf.  If the entry holds a `.link` (child layer),
//!   recurse into it.
//!
//! * **Insertion** — locate the target leaf; if a key-slice collision
//!   occurs with a different full key, create a child layer at
//!   `depth + 1` and migrate both entries.  Handles leaf and interior
//!   node splitting when nodes overflow.
//!
//! * **Deletion** — locate the entry and remove it.  No rebalancing is
//!   performed; nodes may become underfull.
//!
//! * **Iteration** — walk the doubly-linked leaf list from left to
//!   right.
//!
//! ## Memory ownership
//!
//! The layer **owns** every node and key-copy it allocates.  `deinit`
//! recursively frees child layers, interior nodes, leaf nodes, and
//! duplicated key buffers.

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

const config = @import("config.zig");
const key_mod = @import("key.zig");
const leaf_mod = @import("leaf.zig");
const interior_mod = @import("interior.zig");

const KeySlice = key_mod.Slice;
const LeafNode = leaf_mod.LeafNode;
const Entry = leaf_mod.Entry;
const ValueOrLink = leaf_mod.ValueOrLink;
const InteriorNode = interior_mod.InteriorNode;
const ChildPtr = interior_mod.ChildPtr;
const FANOUT = config.FANOUT;

// ─── B⁺ tree root discriminant ──────────────────────────────────────────────

const Root = union(enum) {
    empty: void,
    leaf: *LeafNode,
    interior: *InteriorNode,
};

// ─── Split result propagated upward through recursive insertion ──────────────

pub const SplitResult = struct {
    split_key: KeySlice,
    new_child: ChildPtr,
};

// ─── Layer ───────────────────────────────────────────────────────────────────

pub const Layer = struct {
    root: Root = .empty,
    /// Which 8-byte chunk of the key this layer handles (0-indexed).
    depth: usize,
    allocator: Allocator,

    // ── Construction / destruction ───────────────────────────────────

    pub fn create(allocator: Allocator, depth: usize) !*Layer {
        const self = try allocator.create(Layer);
        self.* = Layer{
            .depth = depth,
            .allocator = allocator,
        };
        return self;
    }

    /// Recursively free **every** resource owned by this layer.
    pub fn deinit(self: *Layer) void {
        switch (self.root) {
            .empty => {},
            .leaf => |l| self.free_leaf(l),
            .interior => |i| self.free_interior(i),
        }
        self.allocator.destroy(self);
    }

    fn free_leaf(self: *Layer, leaf: *LeafNode) void {
        for (leaf.entries[0..leaf.n_keys]) |maybe| {
            if (maybe) |e| {
                switch (e.val) {
                    .link => |ptr| {
                        const sub: *Layer = @ptrCast(@alignCast(ptr));
                        sub.deinit();
                    },
                    .value => {},
                }
                self.allocator.free(e.full_key);
            }
        }
        leaf.destroy(self.allocator);
    }

    fn free_interior(self: *Layer, node: *InteriorNode) void {
        for (node.children[0 .. node.n_keys + 1]) |maybe| {
            if (maybe) |child| switch (child) {
                .leaf => |l| self.free_leaf(l),
                .interior => |i| self.free_interior(i),
            };
        }
        node.destroy(self.allocator);
    }

    // ── Lookup ───────────────────────────────────────────────────────

    /// Retrieve the value associated with `full_key`, or `null`.
    pub fn get(self: *const Layer, full_key: []const u8) ?usize {
        const ks = key_mod.make_slice(full_key, self.depth);
        const leaf = self.find_leaf(ks) orelse return null;
        const res = leaf.find_pos(ks);
        if (!res.found) return null;
        const entry = leaf.const_entry_at(res.idx);
        return switch (entry.val) {
            .value => |v| if (mem.eql(u8, entry.full_key, full_key)) v else null,
            .link => |ptr| {
                const sub: *const Layer = @ptrCast(@alignCast(ptr));
                return sub.get(full_key);
            },
        };
    }

    fn find_leaf(self: *const Layer, ks: KeySlice) ?*LeafNode {
        return switch (self.root) {
            .empty => null,
            .leaf => |l| l,
            .interior => |i| descend_to_leaf(i, ks),
        };
    }

    fn descend_to_leaf(start: *InteriorNode, ks: KeySlice) ?*LeafNode {
        var cur = start;
        while (true) {
            const idx = cur.find_child_idx(ks);
            const child = cur.children[idx] orelse return null;
            switch (child) {
                .leaf => |l| return l,
                .interior => |i| cur = i,
            }
        }
    }

    // ── Insertion ────────────────────────────────────────────────────

    /// Insert or update `(full_key, value)`.
    /// Returns `true` when a new entry was created, `false` when an
    /// existing entry was updated.
    pub fn put(self: *Layer, full_key: []const u8, value: usize) Allocator.Error!bool {
        var was_new: bool = true;
        const ks = key_mod.make_slice(full_key, self.depth);
        switch (self.root) {
            .empty => {
                const leaf = try LeafNode.create(self.allocator);
                const kc = try self.allocator.dupe(u8, full_key);
                leaf.insert_at(0, .{
                    .key_slice = ks,
                    .full_key = kc,
                    .val = .{ .value = value },
                    .key_len = full_key.len,
                });
                self.root = .{ .leaf = leaf };
            },
            .leaf => |leaf| {
                if (try self.insert_into_leaf(leaf, ks, full_key, value, &was_new)) |split| {
                    const nr = try InteriorNode.create(self.allocator);
                    nr.keys[0] = split.split_key;
                    nr.children[0] = .{ .leaf = leaf };
                    nr.children[1] = split.new_child;
                    nr.n_keys = 1;
                    self.root = .{ .interior = nr };
                }
            },
            .interior => |inode| {
                if (try self.insert_into_interior(inode, ks, full_key, value, &was_new)) |split| {
                    const nr = try InteriorNode.create(self.allocator);
                    nr.keys[0] = split.split_key;
                    nr.children[0] = .{ .interior = inode };
                    nr.children[1] = split.new_child;
                    nr.n_keys = 1;
                    self.root = .{ .interior = nr };
                }
            },
        }
        return was_new;
    }

    fn insert_into_leaf(
        self: *Layer,
        leaf: *LeafNode,
        ks: KeySlice,
        full_key: []const u8,
        value: usize,
        was_new: *bool,
    ) Allocator.Error!?SplitResult {
        const res = leaf.find_pos(ks);

        if (res.found) {
            const entry = leaf.entry_at(res.idx);
            switch (entry.val) {
                .value => {
                    if (mem.eql(u8, entry.full_key, full_key)) {
                        // Exact duplicate — update in place.
                        entry.val = .{ .value = value };
                        was_new.* = false;
                        return null;
                    }
                    // Same key-slice, different full key → sublayer.
                    const sub = try Layer.create(self.allocator, self.depth + 1);
                    const old_v = entry.val.value;
                    const old_k = entry.full_key;
                    _ = try sub.put(old_k, old_v);
                    _ = try sub.put(full_key, value);
                    entry.val = .{ .link = @ptrCast(sub) };
                    return null;
                },
                .link => |ptr| {
                    const sub: *Layer = @ptrCast(@alignCast(ptr));
                    was_new.* = try sub.put(full_key, value);
                    return null;
                },
            }
        }

        // New entry — insert if room.
        if (!leaf.is_full()) {
            const kc = try self.allocator.dupe(u8, full_key);
            leaf.insert_at(res.idx, .{
                .key_slice = ks,
                .full_key = kc,
                .val = .{ .value = value },
                .key_len = full_key.len,
            });
            return null;
        }

        // Leaf full — split.
        return try self.split_leaf(leaf, res.idx, ks, full_key, value);
    }

    /// Split `leaf` and return the promoted separator + new right node.
    fn split_leaf(
        self: *Layer,
        leaf: *LeafNode,
        ins_pos: usize,
        ks: KeySlice,
        full_key: []const u8,
        value: usize,
    ) Allocator.Error!SplitResult {
        const new_leaf = try LeafNode.create(self.allocator);

        // Maintain the doubly-linked list.
        new_leaf.next = leaf.next;
        new_leaf.prev = leaf;
        if (leaf.next) |nxt| nxt.prev = new_leaf;
        leaf.next = new_leaf;

        // Build a temporary array of FANOUT + 1 entries (existing + new).
        const total = FANOUT + 1;
        var all: [FANOUT + 1]Entry = undefined;
        var ai: usize = 0;
        for (0..FANOUT) |i| {
            if (ai == ins_pos) {
                all[ai] = .{
                    .key_slice = ks,
                    .full_key = try self.allocator.dupe(u8, full_key),
                    .val = .{ .value = value },
                    .key_len = full_key.len,
                };
                ai += 1;
            }
            all[ai] = leaf.entries[i].?;
            ai += 1;
        }
        if (ai == ins_pos) {
            all[ai] = .{
                .key_slice = ks,
                .full_key = try self.allocator.dupe(u8, full_key),
                .val = .{ .value = value },
                .key_len = full_key.len,
            };
            ai += 1;
        }
        std.debug.assert(ai == total);

        const mid = FANOUT / 2;

        // Left half → original leaf.
        leaf.n_keys = 0;
        for (0..mid) |i| {
            leaf.entries[i] = all[i];
            leaf.n_keys += 1;
        }
        for (mid..FANOUT) |i| leaf.entries[i] = null;

        // Right half → new leaf.
        new_leaf.n_keys = 0;
        for (mid..total) |i| {
            new_leaf.entries[i - mid] = all[i];
            new_leaf.n_keys += 1;
        }

        return .{
            .split_key = all[mid].key_slice,
            .new_child = .{ .leaf = new_leaf },
        };
    }

    fn insert_into_interior(
        self: *Layer,
        node: *InteriorNode,
        ks: KeySlice,
        full_key: []const u8,
        value: usize,
        was_new: *bool,
    ) Allocator.Error!?SplitResult {
        const ci = node.find_child_idx(ks);
        const child = node.children[ci] orelse unreachable;

        const maybe_split: ?SplitResult = switch (child) {
            .leaf => |l| try self.insert_into_leaf(l, ks, full_key, value, was_new),
            .interior => |i| try self.insert_into_interior(i, ks, full_key, value, was_new),
        };

        const split = maybe_split orelse return null;

        if (!node.is_full()) {
            node.insert_at(ci, split.split_key, split.new_child);
            return null;
        }

        return try self.split_interior(node, ci, split.split_key, split.new_child);
    }

    fn split_interior(
        self: *Layer,
        node: *InteriorNode,
        ins_pos: usize,
        new_key: KeySlice,
        new_child: ChildPtr,
    ) Allocator.Error!SplitResult {
        const new_node = try InteriorNode.create(self.allocator);

        // Temporary arrays: FANOUT+1 keys, FANOUT+2 children.
        var all_keys: [FANOUT + 1]KeySlice = undefined;
        var all_children: [FANOUT + 2]?ChildPtr = undefined;

        var ki: usize = 0;
        for (0..FANOUT) |i| {
            if (i == ins_pos) {
                all_keys[ki] = new_key;
                all_children[ki] = node.children[i];
                ki += 1;
                all_children[ki] = new_child;
                all_keys[ki] = node.keys[i];
                ki += 1;
            } else {
                all_keys[ki] = node.keys[i];
                all_children[ki] = node.children[i];
                ki += 1;
            }
        }
        if (ins_pos == FANOUT) {
            all_keys[ki] = new_key;
            all_children[ki] = node.children[FANOUT];
            ki += 1;
            all_children[ki] = new_child;
        } else {
            all_children[ki] = node.children[FANOUT];
        }

        const mid = FANOUT / 2;
        const promote = all_keys[mid];

        // Left half → original node.
        node.n_keys = mid;
        for (0..mid) |i| {
            node.keys[i] = all_keys[i];
            node.children[i] = all_children[i];
        }
        node.children[mid] = all_children[mid];
        for (mid..FANOUT) |i| {
            node.keys[i] = 0;
            node.children[i + 1] = null;
        }

        // Right half → new node.
        const rn = FANOUT - mid;
        new_node.n_keys = rn;
        for (0..rn) |i| {
            new_node.keys[i] = all_keys[mid + 1 + i];
            new_node.children[i] = all_children[mid + 1 + i];
        }
        new_node.children[rn] = all_children[mid + 1 + rn];

        return .{
            .split_key = promote,
            .new_child = .{ .interior = new_node },
        };
    }

    // ── Deletion ─────────────────────────────────────────────────────

    /// Remove `full_key`.  Returns `true` if the key was found and
    /// removed.
    ///
    /// **Note:** no node merging / redistribution is performed.
    pub fn remove(self: *Layer, full_key: []const u8) bool {
        const ks = key_mod.make_slice(full_key, self.depth);
        const leaf = self.find_leaf(ks) orelse return false;
        return self.remove_from_leaf(leaf, ks, full_key);
    }

    fn remove_from_leaf(self: *Layer, leaf: *LeafNode, ks: KeySlice, full_key: []const u8) bool {
        const res = leaf.find_pos(ks);
        if (!res.found) return false;
        const entry = leaf.const_entry_at(res.idx);
        switch (entry.val) {
            .value => {
                if (!mem.eql(u8, entry.full_key, full_key)) return false;
                self.allocator.free(entry.full_key);
                _ = leaf.remove_at(res.idx);
                return true;
            },
            .link => |ptr| {
                const sub: *Layer = @ptrCast(@alignCast(ptr));
                return sub.remove(full_key);
            },
        }
    }

    // ── Iteration ────────────────────────────────────────────────────

    /// Forward iterator over leaf entries at this trie layer.
    pub const Iterator = struct {
        current_leaf: ?*LeafNode,
        current_idx: usize,

        /// Advance and return the next entry, or `null` when exhausted.
        pub fn next(self: *Iterator) ?Entry {
            while (self.current_leaf) |leaf| {
                if (self.current_idx < leaf.n_keys) {
                    const e = leaf.const_entry_at(self.current_idx);
                    self.current_idx += 1;
                    return e;
                }
                self.current_leaf = leaf.next;
                self.current_idx = 0;
            }
            return null;
        }
    };

    /// Create an iterator starting from the leftmost leaf.
    pub fn iterator(self: *const Layer) Iterator {
        return .{
            .current_leaf = self.find_leftmost_leaf(),
            .current_idx = 0,
        };
    }

    fn find_leftmost_leaf(self: *const Layer) ?*LeafNode {
        return switch (self.root) {
            .empty => null,
            .leaf => |l| l,
            .interior => |i| blk: {
                var cur: ChildPtr = .{ .interior = i };
                while (true) switch (cur) {
                    .leaf => |l| break :blk l,
                    .interior => |n| cur = n.children[0] orelse break :blk null,
                };
            },
        };
    }
};

// ─── Inline unit tests ──────────────────────────────────────────────────────

const testing = std.testing;

test "Layer: put and get single entry" {
    var layer = try Layer.create(testing.allocator, 0);
    defer layer.deinit();

    _ = try layer.put("hello", 42);
    try testing.expectEqual(@as(?usize, 42), layer.get("hello"));
}

test "Layer: get missing returns null" {
    var layer = try Layer.create(testing.allocator, 0);
    defer layer.deinit();
    try testing.expectEqual(@as(?usize, null), layer.get("missing"));
}

test "Layer: update in place" {
    var layer = try Layer.create(testing.allocator, 0);
    defer layer.deinit();

    _ = try layer.put("k", 1);
    _ = try layer.put("k", 2);
    try testing.expectEqual(@as(?usize, 2), layer.get("k"));
}

test "Layer: remove" {
    var layer = try Layer.create(testing.allocator, 0);
    defer layer.deinit();

    _ = try layer.put("k", 1);
    try testing.expect(layer.remove("k"));
    try testing.expectEqual(@as(?usize, null), layer.get("k"));
    try testing.expect(!layer.remove("k"));
}

test "Layer: split triggered by FANOUT+1 inserts" {
    var layer = try Layer.create(testing.allocator, 0);
    defer layer.deinit();

    for (0..FANOUT + 1) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "key_{d:0>4}", .{i}) catch unreachable;
        _ = try layer.put(k, i);
    }
    for (0..FANOUT + 1) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "key_{d:0>4}", .{i}) catch unreachable;
        try testing.expectEqual(@as(?usize, i), layer.get(k));
    }
}

test "Layer: sublayer creation for colliding key slices" {
    var layer = try Layer.create(testing.allocator, 0);
    defer layer.deinit();

    _ = try layer.put("abcdefghXXX", 1);
    _ = try layer.put("abcdefghYYY", 2);
    try testing.expectEqual(@as(?usize, 1), layer.get("abcdefghXXX"));
    try testing.expectEqual(@as(?usize, 2), layer.get("abcdefghYYY"));
}
