//! Extended tests for `src/interior.zig` — InteriorNode operations.

const std = @import("std");
const testing = std.testing;
const masstree = @import("masstree");
const InteriorNode = masstree.InteriorNode;
const ChildPtr = masstree.ChildPtr;
const LeafNode = masstree.LeafNode;
const KeySlice = masstree.key.Slice;
const config = masstree.config;

// ── create / destroy ─────────────────────────────────────────────────────────

test "create and destroy" {
    const node = try InteriorNode.create(testing.allocator);
    defer node.destroy(testing.allocator);

    try testing.expectEqual(@as(usize, 0), node.n_keys);
    try testing.expect(!node.is_full());
}

// ── find_child_idx boundary ────────────────────────────────────────────────────

test "find_child_idx: all boundaries" {
    var node = InteriorNode{};
    node.keys[0] = 10;
    node.keys[1] = 20;
    node.keys[2] = 30;
    node.n_keys = 3;

    try testing.expectEqual(@as(usize, 0), node.find_child_idx(5));
    try testing.expectEqual(@as(usize, 1), node.find_child_idx(10));
    try testing.expectEqual(@as(usize, 1), node.find_child_idx(15));
    try testing.expectEqual(@as(usize, 2), node.find_child_idx(20));
    try testing.expectEqual(@as(usize, 3), node.find_child_idx(30));
    try testing.expectEqual(@as(usize, 3), node.find_child_idx(999));
}

test "find_child_idx: empty node always returns 0" {
    const node = InteriorNode{};
    try testing.expectEqual(@as(usize, 0), node.find_child_idx(42));
}

test "find_child_idx: single separator" {
    var node = InteriorNode{};
    node.keys[0] = 500;
    node.n_keys = 1;

    try testing.expectEqual(@as(usize, 0), node.find_child_idx(499));
    try testing.expectEqual(@as(usize, 1), node.find_child_idx(500));
    try testing.expectEqual(@as(usize, 1), node.find_child_idx(501));
}

// ── insert_at ─────────────────────────────────────────────────────────────────

test "insert_at preserves child pointers" {
    var node = InteriorNode{};
    const l0 = try LeafNode.create(testing.allocator);
    defer l0.destroy(testing.allocator);
    const l1 = try LeafNode.create(testing.allocator);
    defer l1.destroy(testing.allocator);
    const l2 = try LeafNode.create(testing.allocator);
    defer l2.destroy(testing.allocator);

    node.children[0] = .{ .leaf = l0 };
    node.insert_at(0, 100, .{ .leaf = l1 });
    node.insert_at(0, 50, .{ .leaf = l2 });

    try testing.expectEqual(@as(usize, 2), node.n_keys);
    try testing.expectEqual(@as(u64, 50), node.keys[0]);
    try testing.expectEqual(@as(u64, 100), node.keys[1]);
    try testing.expectEqual(l0, node.children[0].?.leaf);
    try testing.expectEqual(l2, node.children[1].?.leaf);
    try testing.expectEqual(l1, node.children[2].?.leaf);
}

// ── is_full ───────────────────────────────────────────────────────────────────

test "is_full at capacity" {
    var node = InteriorNode{};
    node.n_keys = config.FANOUT - 1;
    try testing.expect(!node.is_full());
    node.n_keys = config.FANOUT;
    try testing.expect(node.is_full());
}
