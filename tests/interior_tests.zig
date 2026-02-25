//! Extended tests for `src/interior.zig` — InteriorNode operations.

const std = @import("std");
const testing = std.testing;
const InteriorNode = @import("../src/interior.zig").InteriorNode;
const ChildPtr = @import("../src/interior.zig").ChildPtr;
const LeafNode = @import("../src/leaf.zig").LeafNode;
const KeySlice = @import("../src/leaf.zig").KeySlice;
const config = @import("../src/config.zig");

// ── create / destroy ─────────────────────────────────────────────────────────

test "create and destroy" {
    const node = try InteriorNode.create(testing.allocator);
    defer testing.allocator.destroy(node);

    try testing.expectEqual(@as(usize, 0), node.n_keys);
    try testing.expect(!node.isFull());
}

// ── findChildIdx boundary ────────────────────────────────────────────────────

test "findChildIdx: all boundaries" {
    var node = InteriorNode{};
    node.keys[0] = 10;
    node.keys[1] = 20;
    node.keys[2] = 30;
    node.n_keys = 3;

    try testing.expectEqual(@as(usize, 0), node.findChildIdx(5));
    try testing.expectEqual(@as(usize, 1), node.findChildIdx(10));
    try testing.expectEqual(@as(usize, 1), node.findChildIdx(15));
    try testing.expectEqual(@as(usize, 2), node.findChildIdx(20));
    try testing.expectEqual(@as(usize, 3), node.findChildIdx(30));
    try testing.expectEqual(@as(usize, 3), node.findChildIdx(999));
}

test "findChildIdx: empty node always returns 0" {
    const node = InteriorNode{};
    try testing.expectEqual(@as(usize, 0), node.findChildIdx(42));
}

test "findChildIdx: single separator" {
    var node = InteriorNode{};
    node.keys[0] = 500;
    node.n_keys = 1;

    try testing.expectEqual(@as(usize, 0), node.findChildIdx(499));
    try testing.expectEqual(@as(usize, 1), node.findChildIdx(500));
    try testing.expectEqual(@as(usize, 1), node.findChildIdx(501));
}

// ── insertAt ─────────────────────────────────────────────────────────────────

test "insertAt preserves child pointers" {
    var node = InteriorNode{};
    const l0 = try LeafNode.create(testing.allocator);
    defer testing.allocator.destroy(l0);
    const l1 = try LeafNode.create(testing.allocator);
    defer testing.allocator.destroy(l1);
    const l2 = try LeafNode.create(testing.allocator);
    defer testing.allocator.destroy(l2);

    node.children[0] = .{ .leaf = l0 };
    node.insertAt(0, 100, .{ .leaf = l1 });
    node.insertAt(0, 50, .{ .leaf = l2 });

    try testing.expectEqual(@as(usize, 2), node.n_keys);
    try testing.expectEqual(@as(u64, 50), node.keys[0]);
    try testing.expectEqual(@as(u64, 100), node.keys[1]);
    try testing.expectEqual(l0, node.children[0].?.leaf);
    try testing.expectEqual(l2, node.children[1].?.leaf);
    try testing.expectEqual(l1, node.children[2].?.leaf);
}

// ── isFull ───────────────────────────────────────────────────────────────────

test "isFull at capacity" {
    var node = InteriorNode{};
    node.n_keys = config.FANOUT - 1;
    try testing.expect(!node.isFull());
    node.n_keys = config.FANOUT;
    try testing.expect(node.isFull());
}
