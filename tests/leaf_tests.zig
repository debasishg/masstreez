//! Extended tests for `src/leaf.zig` — LeafNode operations.

const std = @import("std");
const testing = std.testing;
const masstree = @import("masstree");
const LeafNode = masstree.LeafNode;
const Entry = masstree.Entry;
const KeySlice = masstree.key.Slice;
const config = masstree.config;

// ── create / destroy ─────────────────────────────────────────────────────────

test "create and destroy empty leaf" {
    const node = try LeafNode.create(testing.allocator);
    defer node.destroy(testing.allocator);

    try testing.expectEqual(@as(usize, 0), node.n_keys);
    try testing.expect(!node.is_full());
}

// ── insert ordering ──────────────────────────────────────────────────────────

test "insert_at maintains sort order when inserting in reverse" {
    var node = LeafNode{};
    node.insert_at(0, .{ .key_slice = 300, .full_key = "c", .val = .{ .value = 3 }, .key_len = 1 });
    node.insert_at(0, .{ .key_slice = 100, .full_key = "a", .val = .{ .value = 1 }, .key_len = 1 });
    node.insert_at(1, .{ .key_slice = 200, .full_key = "b", .val = .{ .value = 2 }, .key_len = 1 });

    try testing.expectEqual(@as(KeySlice, 100), node.const_entry_at(0).key_slice);
    try testing.expectEqual(@as(KeySlice, 200), node.const_entry_at(1).key_slice);
    try testing.expectEqual(@as(KeySlice, 300), node.const_entry_at(2).key_slice);
}

// ── remove_at positions ───────────────────────────────────────────────────────

test "remove_at first, middle, last" {
    var node = LeafNode{};
    for (0..5) |i| {
        node.insert_at(i, .{
            .key_slice = @as(u64, @intCast(i * 10)),
            .full_key = "x",
            .val = .{ .value = i },
            .key_len = 1,
        });
    }

    // Middle
    _ = node.remove_at(2);
    try testing.expectEqual(@as(usize, 4), node.n_keys);
    try testing.expectEqual(@as(u64, 30), node.const_entry_at(2).key_slice);

    // First
    _ = node.remove_at(0);
    try testing.expectEqual(@as(usize, 3), node.n_keys);
    try testing.expectEqual(@as(u64, 10), node.const_entry_at(0).key_slice);

    // Last
    _ = node.remove_at(2);
    try testing.expectEqual(@as(usize, 2), node.n_keys);
}

// ── find_pos with duplicate slices ────────────────────────────────────────────

test "find_pos returns first entry with matching key_slice" {
    var node = LeafNode{};
    node.insert_at(0, .{ .key_slice = 100, .full_key = "aaa", .val = .{ .value = 1 }, .key_len = 3 });
    // In the real masstree, a second entry with the same key_slice would
    // be routed to a sublayer, so we don't store two entries with the
    // same key_slice.  find_pos simply locates the first match.

    const r1 = node.find_pos(100);
    try testing.expect(r1.found);
    try testing.expectEqual(@as(usize, 0), r1.idx);

    // Non-existent slice returns insertion point.
    const r2 = node.find_pos(50);
    try testing.expect(!r2.found);
    try testing.expectEqual(@as(usize, 0), r2.idx);

    const r3 = node.find_pos(200);
    try testing.expect(!r3.found);
    try testing.expectEqual(@as(usize, 1), r3.idx);
}

// ── fill to capacity ─────────────────────────────────────────────────────────

test "fill to FANOUT and verify is_full" {
    var node = LeafNode{};
    for (0..config.FANOUT) |i| {
        try testing.expect(!node.is_full());
        node.insert_at(i, .{
            .key_slice = @as(KeySlice, @intCast(i)),
            .full_key = "k",
            .val = .{ .value = i },
            .key_len = 1,
        });
    }
    try testing.expect(node.is_full());
    try testing.expectEqual(@as(usize, config.FANOUT), node.n_keys);
}

// ── entry_at / const_entry_at ───────────────────────────────────────────────────

test "entry_at returns mutable reference" {
    var node = LeafNode{};
    node.insert_at(0, .{ .key_slice = 1, .full_key = "a", .val = .{ .value = 10 }, .key_len = 1 });

    const e = node.entry_at(0);
    e.val = .{ .value = 99 };
    try testing.expectEqual(@as(usize, 99), node.const_entry_at(0).val.value);
}
