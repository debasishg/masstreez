//! B⁺ tree **leaf node** for one Masstree trie layer.
//!
//! Each leaf holds up to `FANOUT` entries sorted by
//! `(key_slice, full_key)`.  Entries that share the same 8-byte key
//! slice but differ in their full keys are kept adjacent; the layer
//! will transparently create a child trie layer to disambiguate them.
//!
//! Leaf nodes are linked in a **doubly-linked list** so that range
//! iteration can walk from one leaf to the next without re-traversing
//! the interior nodes.

const std = @import("std");
const mem = std.mem;
const config = @import("config.zig");
const key_mod = @import("key.zig");

pub const KeySlice = key_mod.Slice;

// ─── Value payload ───────────────────────────────────────────────────────────

/// The payload of a leaf entry.
///
/// * `.value` — a terminal `usize` payload.
/// * `.link`  — an opaque pointer to a child `Layer` for keys that
///              extend beyond the current trie depth.  Stored as
///              `*anyopaque` to break the circular `leaf ↔ layer`
///              import; the cast is confined to `layer.zig`.
pub const ValueOrLink = union(enum) {
    value: usize,
    link: *anyopaque, // actually *Layer, cast in layer.zig
};

// ─── Leaf entry ──────────────────────────────────────────────────────────────

/// A single sorted entry inside a leaf node.
pub const Entry = struct {
    /// The 8-byte slice at the current trie depth.
    key_slice: KeySlice,
    /// Owned copy of the **complete** key (used for disambiguation).
    full_key: []const u8,
    /// Payload — either a direct value or a link to the next trie layer.
    val: ValueOrLink,
    /// Original length of the key (informational).
    key_len: usize,
};

// ─── Leaf node ───────────────────────────────────────────────────────────────

/// A B⁺ tree leaf node holding up to `FANOUT` entries in sorted order.
pub const LeafNode = struct {
    /// Current number of live entries.
    n_keys: usize = 0,
    /// Entry slots — first `n_keys` are populated, rest are `null`.
    entries: [config.FANOUT]?Entry = [_]?Entry{null} ** config.FANOUT,
    /// Forward pointer for the leaf linked-list.
    next: ?*LeafNode = null,
    /// Backward pointer for the leaf linked-list.
    prev: ?*LeafNode = null,

    /// Heap-allocate a fresh, empty leaf.
    pub fn create(allocator: mem.Allocator) !*LeafNode {
        const node = try allocator.create(LeafNode);
        node.* = LeafNode{};
        return node;
    }

    /// Return this node's memory to the allocator.
    pub fn destroy(self: *LeafNode, allocator: mem.Allocator) void {
        allocator.destroy(self);
    }

    /// Find the position for `ks` within this leaf.
    ///
    /// Returns an `(idx, found)` pair:
    /// * If `found` is `true`, `idx` is the index of the entry whose
    ///   `key_slice` equals `ks`.
    /// * Otherwise `idx` is the insertion point that preserves sort order.
    pub fn find_pos(self: *const LeafNode, ks: KeySlice) struct { idx: usize, found: bool } {
        var i: usize = 0;
        while (i < self.n_keys) : (i += 1) {
            const e = self.entries[i].?;
            if (e.key_slice == ks) {
                return .{ .idx = i, .found = true };
            } else if (e.key_slice > ks) {
                return .{ .idx = i, .found = false };
            }
        }
        return .{ .idx = self.n_keys, .found = false };
    }

    /// Insert `entry` at position `pos`, shifting later entries right.
    ///
    /// **Precondition:** the node must not be full.
    pub fn insert_at(self: *LeafNode, pos: usize, entry: Entry) void {
        std.debug.assert(self.n_keys < config.FANOUT);
        var i: usize = self.n_keys;
        while (i > pos) : (i -= 1) {
            self.entries[i] = self.entries[i - 1];
        }
        self.entries[pos] = entry;
        self.n_keys += 1;
    }

    /// Remove and return the entry at `pos`, shifting later entries left.
    pub fn remove_at(self: *LeafNode, pos: usize) Entry {
        std.debug.assert(pos < self.n_keys);
        const entry = self.entries[pos].?;
        var i: usize = pos;
        while (i + 1 < self.n_keys) : (i += 1) {
            self.entries[i] = self.entries[i + 1];
        }
        self.entries[self.n_keys - 1] = null;
        self.n_keys -= 1;
        return entry;
    }

    /// `true` when no more entries can be added without splitting.
    pub fn is_full(self: *const LeafNode) bool {
        return self.n_keys >= config.FANOUT;
    }

    /// Mutable reference to the entry at `pos`.
    pub fn entry_at(self: *LeafNode, pos: usize) *Entry {
        return &self.entries[pos].?;
    }

    /// Value (copy) of the entry at `pos`.
    pub fn const_entry_at(self: *const LeafNode, pos: usize) Entry {
        return self.entries[pos].?;
    }
};

// ─── Inline unit tests ──────────────────────────────────────────────────────

const testing = std.testing;

test "LeafNode: insert_at and find_pos" {
    var node = LeafNode{};
    node.insert_at(0, .{ .key_slice = 100, .full_key = "aaa", .val = .{ .value = 1 }, .key_len = 3 });
    node.insert_at(1, .{ .key_slice = 200, .full_key = "bbb", .val = .{ .value = 2 }, .key_len = 3 });
    node.insert_at(2, .{ .key_slice = 300, .full_key = "ccc", .val = .{ .value = 3 }, .key_len = 3 });
    try testing.expectEqual(@as(usize, 3), node.n_keys);

    const r1 = node.find_pos(200);
    try testing.expect(r1.found);
    try testing.expectEqual(@as(usize, 1), r1.idx);

    const r2 = node.find_pos(150);
    try testing.expect(!r2.found);
    try testing.expectEqual(@as(usize, 1), r2.idx);

    const r3 = node.find_pos(400);
    try testing.expect(!r3.found);
    try testing.expectEqual(@as(usize, 3), r3.idx);
}

test "LeafNode: remove_at" {
    var node = LeafNode{};
    node.insert_at(0, .{ .key_slice = 10, .full_key = "a", .val = .{ .value = 1 }, .key_len = 1 });
    node.insert_at(1, .{ .key_slice = 20, .full_key = "b", .val = .{ .value = 2 }, .key_len = 1 });
    node.insert_at(2, .{ .key_slice = 30, .full_key = "c", .val = .{ .value = 3 }, .key_len = 1 });

    const removed = node.remove_at(1);
    try testing.expectEqual(@as(usize, 20), removed.key_slice);
    try testing.expectEqual(@as(usize, 2), node.n_keys);
    try testing.expectEqual(@as(usize, 30), node.const_entry_at(1).key_slice);
}

test "LeafNode: is_full" {
    var node = LeafNode{};
    for (0..config.FANOUT) |i| {
        try testing.expect(!node.is_full());
        node.insert_at(i, .{
            .key_slice = @as(KeySlice, @intCast(i)),
            .full_key = "x",
            .val = .{ .value = i },
            .key_len = 1,
        });
    }
    try testing.expect(node.is_full());
}
