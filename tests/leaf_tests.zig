//! Extended tests for `src/leaf.zig` — LeafNode operations.

const std = @import("std");
const testing = std.testing;
const LeafNode = @import("../src/leaf.zig").LeafNode;
const Entry = @import("../src/leaf.zig").Entry;
const KeySlice = @import("../src/leaf.zig").KeySlice;
const config = @import("../src/config.zig");

// ── create / destroy ─────────────────────────────────────────────────────────

test "create and destroy empty leaf" {
    const node = try LeafNode.create(testing.allocator);
    defer testing.allocator.destroy(node);

    try testing.expectEqual(@as(usize, 0), node.n_keys);
    try testing.expect(!node.isFull());
}

// ── insert ordering ──────────────────────────────────────────────────────────

test "insertAt maintains sort order when inserting in reverse" {
    var node = LeafNode{};
    node.insertAt(0, .{ .key_slice = 300, .full_key = "c", .val = .{ .value = 3 }, .key_len = 1 });
    node.insertAt(0, .{ .key_slice = 100, .full_key = "a", .val = .{ .value = 1 }, .key_len = 1 });
    node.insertAt(1, .{ .key_slice = 200, .full_key = "b", .val = .{ .value = 2 }, .key_len = 1 });

    try testing.expectEqual(@as(KeySlice, 100), node.constEntryAt(0).key_slice);
    try testing.expectEqual(@as(KeySlice, 200), node.constEntryAt(1).key_slice);
    try testing.expectEqual(@as(KeySlice, 300), node.constEntryAt(2).key_slice);
}

// ── removeAt positions ───────────────────────────────────────────────────────

test "removeAt first, middle, last" {
    var node = LeafNode{};
    for (0..5) |i| {
        node.insertAt(i, .{
            .key_slice = @as(u64, @intCast(i * 10)),
            .full_key = "x",
            .val = .{ .value = i },
            .key_len = 1,
        });
    }

    // Middle
    _ = node.removeAt(2);
    try testing.expectEqual(@as(usize, 4), node.n_keys);
    try testing.expectEqual(@as(u64, 30), node.constEntryAt(2).key_slice);

    // First
    _ = node.removeAt(0);
    try testing.expectEqual(@as(usize, 3), node.n_keys);
    try testing.expectEqual(@as(u64, 10), node.constEntryAt(0).key_slice);

    // Last
    _ = node.removeAt(2);
    try testing.expectEqual(@as(usize, 2), node.n_keys);
}

// ── findPos with duplicate slices ────────────────────────────────────────────

test "findPos distinguishes entries with same key_slice" {
    var node = LeafNode{};
    node.insertAt(0, .{ .key_slice = 100, .full_key = "aaa", .val = .{ .value = 1 }, .key_len = 3 });
    node.insertAt(1, .{ .key_slice = 100, .full_key = "bbb", .val = .{ .value = 2 }, .key_len = 3 });

    const r1 = node.findPos(100, "aaa");
    try testing.expect(r1.found);
    try testing.expectEqual(@as(usize, 0), r1.idx);

    const r2 = node.findPos(100, "bbb");
    try testing.expect(r2.found);
    try testing.expectEqual(@as(usize, 1), r2.idx);

    // Between the two
    const r3 = node.findPos(100, "abc");
    try testing.expect(!r3.found);
    try testing.expectEqual(@as(usize, 1), r3.idx);
}

// ── fill to capacity ─────────────────────────────────────────────────────────

test "fill to FANOUT and verify isFull" {
    var node = LeafNode{};
    for (0..config.FANOUT) |i| {
        try testing.expect(!node.isFull());
        node.insertAt(i, .{
            .key_slice = @as(KeySlice, @intCast(i)),
            .full_key = "k",
            .val = .{ .value = i },
            .key_len = 1,
        });
    }
    try testing.expect(node.isFull());
    try testing.expectEqual(@as(usize, config.FANOUT), node.n_keys);
}

// ── entryAt / constEntryAt ───────────────────────────────────────────────────

test "entryAt returns mutable reference" {
    var node = LeafNode{};
    node.insertAt(0, .{ .key_slice = 1, .full_key = "a", .val = .{ .value = 10 }, .key_len = 1 });

    const e = node.entryAt(0);
    e.val = .{ .value = 99 };
    try testing.expectEqual(@as(usize, 99), node.constEntryAt(0).val.value);
}
