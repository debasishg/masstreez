//! B⁺ tree **interior (branch) node** for one Masstree trie layer.
//!
//! An interior node stores up to `FANOUT` separator keys and
//! `FANOUT + 1` child pointers.  Children may be leaf nodes or
//! other interior nodes (the tree is height-uniform within a layer).
//!
//! Routing follows standard B⁺ tree logic:
//!
//! ```text
//! child[0]  keys[0]  child[1]  keys[1]  …  keys[n-1]  child[n]
//!           ──────>           ──────>       ──────>
//! ```
//!
//! For a search key `k`, follow `child[i]` where
//! `keys[i-1] ≤ k < keys[i]` (with `keys[-1] = -∞`, `keys[n] = +∞`).

const std = @import("std");
const config = @import("config.zig");
const leaf_mod = @import("leaf.zig");

pub const KeySlice = leaf_mod.KeySlice;

/// A child pointer inside an interior node — either another interior
/// or a leaf.
pub const ChildPtr = union(enum) {
    leaf: *leaf_mod.LeafNode,
    interior: *InteriorNode,
};

/// Interior (branch) node of the B⁺ tree.
pub const InteriorNode = struct {
    /// Current number of separator keys.
    n_keys: usize = 0,
    /// Separator keys (first `n_keys` are valid).
    keys: [config.FANOUT]KeySlice = [_]KeySlice{0} ** config.FANOUT,
    /// Child pointers (`n_keys + 1` are valid).
    children: [config.FANOUT + 1]?ChildPtr = [_]?ChildPtr{null} ** (config.FANOUT + 1),

    /// Heap-allocate an empty interior node.
    pub fn create(allocator: std.mem.Allocator) !*InteriorNode {
        const node = try allocator.create(InteriorNode);
        node.* = InteriorNode{};
        return node;
    }

    /// Return this node's memory to the allocator.
    pub fn destroy(self: *InteriorNode, allocator: std.mem.Allocator) void {
        allocator.destroy(self);
    }

    /// Return the child index to follow for `key_slice`.
    ///
    /// For equal keys the search goes **right** (child index `i + 1`),
    /// which matches the standard "lower-bound goes right" B⁺ tree
    /// convention.
    pub fn find_child_idx(self: *const InteriorNode, ks: KeySlice) usize {
        var i: usize = 0;
        while (i < self.n_keys) : (i += 1) {
            if (ks < self.keys[i]) return i;
        }
        return self.n_keys;
    }

    /// Insert separator `sep_key` at position `pos`.
    /// `right_child` becomes `children[pos + 1]`; the existing child
    /// at `pos` remains as the left child.
    ///
    /// **Precondition:** the node must not be full.
    pub fn insert_at(self: *InteriorNode, pos: usize, sep_key: KeySlice, right_child: ChildPtr) void {
        std.debug.assert(self.n_keys < config.FANOUT);
        // Shift keys right
        var i: usize = self.n_keys;
        while (i > pos) : (i -= 1) {
            self.keys[i] = self.keys[i - 1];
        }
        self.keys[pos] = sep_key;
        // Shift children right (from pos+1 onward)
        i = self.n_keys + 1;
        while (i > pos + 1) : (i -= 1) {
            self.children[i] = self.children[i - 1];
        }
        self.children[pos + 1] = right_child;
        self.n_keys += 1;
    }

    /// `true` when no more separators can be added without splitting.
    pub fn is_full(self: *const InteriorNode) bool {
        return self.n_keys >= config.FANOUT;
    }
};

// ─── Inline unit tests ──────────────────────────────────────────────────────

const testing = std.testing;

test "InteriorNode: find_child_idx" {
    var node = InteriorNode{};
    node.keys[0] = 100;
    node.keys[1] = 200;
    node.keys[2] = 300;
    node.n_keys = 3;

    try testing.expectEqual(@as(usize, 0), node.find_child_idx(50));
    try testing.expectEqual(@as(usize, 1), node.find_child_idx(100));
    try testing.expectEqual(@as(usize, 1), node.find_child_idx(150));
    try testing.expectEqual(@as(usize, 2), node.find_child_idx(200));
    try testing.expectEqual(@as(usize, 3), node.find_child_idx(300));
    try testing.expectEqual(@as(usize, 3), node.find_child_idx(400));
}

test "InteriorNode: insert_at" {
    var node = InteriorNode{};
    const dummy_leaf = try leaf_mod.LeafNode.create(testing.allocator);
    defer testing.allocator.destroy(dummy_leaf);
    const dummy_leaf2 = try leaf_mod.LeafNode.create(testing.allocator);
    defer testing.allocator.destroy(dummy_leaf2);
    const dummy_leaf3 = try leaf_mod.LeafNode.create(testing.allocator);
    defer testing.allocator.destroy(dummy_leaf3);

    node.children[0] = .{ .leaf = dummy_leaf };
    node.insert_at(0, 100, .{ .leaf = dummy_leaf2 });
    node.insert_at(0, 50, .{ .leaf = dummy_leaf3 });

    try testing.expectEqual(@as(usize, 2), node.n_keys);
    try testing.expectEqual(@as(u64, 50), node.keys[0]);
    try testing.expectEqual(@as(u64, 100), node.keys[1]);
    try testing.expectEqual(dummy_leaf, node.children[0].?.leaf);
    try testing.expectEqual(dummy_leaf3, node.children[1].?.leaf);
    try testing.expectEqual(dummy_leaf2, node.children[2].?.leaf);
}

test "InteriorNode: is_full" {
    var node = InteriorNode{};
    node.n_keys = config.FANOUT - 1;
    try testing.expect(!node.is_full());
    node.n_keys = config.FANOUT;
    try testing.expect(node.is_full());
}
