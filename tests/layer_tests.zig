//! Extended tests for `src/layer.zig` — the per-depth B⁺ tree.

const std = @import("std");
const testing = std.testing;
const Layer = @import("../src/layer.zig").Layer;
const config = @import("../src/config.zig");

// ── bulk insert / lookup ─────────────────────────────────────────────────────

test "200 inserts and lookups" {
    var layer = try Layer.create(testing.allocator, 0);
    defer layer.deinit();

    const n: usize = 200;
    for (0..n) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "key_{d:0>6}", .{i}) catch unreachable;
        try layer.put(k, i);
    }
    for (0..n) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "key_{d:0>6}", .{i}) catch unreachable;
        try testing.expectEqual(@as(?usize, i), layer.get(k));
    }
}

// ── interleaved insert / remove ──────────────────────────────────────────────

test "insert then remove odd keys" {
    var layer = try Layer.create(testing.allocator, 0);
    defer layer.deinit();

    for (0..50) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "lr_{d:0>4}", .{i}) catch unreachable;
        try layer.put(k, i);
    }

    for (0..50) |i| {
        if (i % 2 == 1) {
            var buf: [32]u8 = undefined;
            const k = std.fmt.bufPrint(&buf, "lr_{d:0>4}", .{i}) catch unreachable;
            try testing.expect(layer.remove(k));
        }
    }

    for (0..50) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "lr_{d:0>4}", .{i}) catch unreachable;
        if (i % 2 == 1) {
            try testing.expectEqual(@as(?usize, null), layer.get(k));
        } else {
            try testing.expectEqual(@as(?usize, i), layer.get(k));
        }
    }
}

// ── sublayer (deep keys) ─────────────────────────────────────────────────────

test "20 keys sharing an 8-byte prefix" {
    var layer = try Layer.create(testing.allocator, 0);
    defer layer.deinit();

    const prefix = "XXXXXXXX"; // exactly 8 bytes
    for (0..20) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "{s}{d:0>4}", .{ prefix, i }) catch unreachable;
        try layer.put(k, i * 100);
    }
    for (0..20) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "{s}{d:0>4}", .{ prefix, i }) catch unreachable;
        try testing.expectEqual(@as(?usize, i * 100), layer.get(k));
    }
}

// ── iterator ─────────────────────────────────────────────────────────────────

test "iterator walks leaves in sorted order" {
    var layer = try Layer.create(testing.allocator, 0);
    defer layer.deinit();

    const n: usize = config.FANOUT * 3;
    for (0..n) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "it_{d:0>4}", .{i}) catch unreachable;
        try layer.put(k, i);
    }

    var it = layer.iterator();
    var count: usize = 0;
    var prev_slice: ?u64 = null;
    while (it.next()) |entry| {
        if (prev_slice) |p| {
            try testing.expect(entry.key_slice >= p);
        }
        prev_slice = entry.key_slice;
        count += 1;
    }
    try testing.expectEqual(n, count);
}

// ── empty layer ──────────────────────────────────────────────────────────────

test "iterator on empty layer" {
    var layer = try Layer.create(testing.allocator, 0);
    defer layer.deinit();

    var it = layer.iterator();
    try testing.expectEqual(@as(?@import("../src/leaf.zig").Entry, null), it.next());
}
