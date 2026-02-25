//! Integration and stress tests exercising the full Masstree through
//! realistic workload patterns.

const std = @import("std");
const testing = std.testing;
const Masstree = @import("../src/tree.zig").Masstree;

// ── stress: 500 keys with mixed ops ──────────────────────────────────────────

test "stress: 500 keys, delete every 3rd, update survivors" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    const n: usize = 500;
    for (0..n) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "stress_{d:0>6}", .{i}) catch unreachable;
        try t.put(k, i);
    }
    try testing.expectEqual(n, t.len());

    // Delete every 3rd key.
    var deleted: usize = 0;
    for (0..n) |i| {
        if (i % 3 == 0) {
            var buf: [32]u8 = undefined;
            const k = std.fmt.bufPrint(&buf, "stress_{d:0>6}", .{i}) catch unreachable;
            try testing.expect(t.remove(k));
            deleted += 1;
        }
    }
    try testing.expectEqual(n - deleted, t.len());

    // Update survivors.
    for (0..n) |i| {
        if (i % 3 != 0) {
            var buf: [32]u8 = undefined;
            const k = std.fmt.bufPrint(&buf, "stress_{d:0>6}", .{i}) catch unreachable;
            try t.put(k, i * 10);
        }
    }

    // Verify.
    for (0..n) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "stress_{d:0>6}", .{i}) catch unreachable;
        if (i % 3 == 0) {
            try testing.expectEqual(@as(?usize, null), t.get(k));
        } else {
            try testing.expectEqual(@as(?usize, i * 10), t.get(k));
        }
    }
}

// ── common-prefix keys at scale ──────────────────────────────────────────────

test "100 keys sharing 8-byte prefix (sublayer stress)" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    for (0..100) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "SHARED__{d:0>6}", .{i}) catch unreachable;
        try t.put(k, i);
    }
    try testing.expectEqual(@as(usize, 100), t.len());

    for (0..100) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "SHARED__{d:0>6}", .{i}) catch unreachable;
        try testing.expectEqual(@as(?usize, i), t.get(k));
    }
}

// ── deep trie (48+ byte keys → 6 layers) ────────────────────────────────────

test "deep trie with 48-byte keys" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    for (0..50) |i| {
        var buf: [64]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "AAAAAAAABBBBBBBBCCCCCCCCDDDDDDDDEEEEEEEE{d:0>6}__", .{i}) catch unreachable;
        try t.put(k, i);
    }
    for (0..50) |i| {
        var buf: [64]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "AAAAAAAABBBBBBBBCCCCCCCCDDDDDDDDEEEEEEEE{d:0>6}__", .{i}) catch unreachable;
        try testing.expectEqual(@as(?usize, i), t.get(k));
    }
}

// ── interleaved insert / delete phases ───────────────────────────────────────

test "interleaved insert and delete phases" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    // Phase 1: bulk insert 0..199
    for (0..200) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "igd_{d:0>5}", .{i}) catch unreachable;
        try t.put(k, i);
    }

    // Phase 2: delete 0..49 while inserting 200..249
    for (0..50) |i| {
        var dbuf: [32]u8 = undefined;
        const dk = std.fmt.bufPrint(&dbuf, "igd_{d:0>5}", .{i}) catch unreachable;
        try testing.expect(t.remove(dk));

        var ibuf: [32]u8 = undefined;
        const ik = std.fmt.bufPrint(&ibuf, "igd_{d:0>5}", .{i + 200}) catch unreachable;
        try t.put(ik, i + 200);
    }

    try testing.expectEqual(@as(usize, 200), t.len());

    // 0..49 gone
    for (0..50) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "igd_{d:0>5}", .{i}) catch unreachable;
        try testing.expectEqual(@as(?usize, null), t.get(k));
    }
    // 50..249 present
    for (50..250) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "igd_{d:0>5}", .{i}) catch unreachable;
        try testing.expectEqual(@as(?usize, i), t.get(k));
    }
}

// ── overwrite burst ──────────────────────────────────────────────────────────

test "overwrite each key 10 times" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    for (0..100) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "ow_{d:0>4}", .{i}) catch unreachable;
        try t.put(k, 0);
    }
    for (1..11) |round| {
        for (0..100) |i| {
            var buf: [32]u8 = undefined;
            const k = std.fmt.bufPrint(&buf, "ow_{d:0>4}", .{i}) catch unreachable;
            try t.put(k, round * 1000 + i);
        }
    }
    try testing.expectEqual(@as(usize, 100), t.len());
    for (0..100) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "ow_{d:0>4}", .{i}) catch unreachable;
        try testing.expectEqual(@as(?usize, 10 * 1000 + i), t.get(k));
    }
}

// ── empty key ────────────────────────────────────────────────────────────────

test "empty string as key" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    try t.put("", 0);
    try testing.expectEqual(@as(?usize, 0), t.get(""));
    try testing.expectEqual(@as(usize, 1), t.len());
    try testing.expect(t.remove(""));
    try testing.expect(t.isEmpty());
}

// ── keys with common 8-byte prefix then diverge ─────────────────────────────

test "trie layer depth transitions" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    try t.put("AAAAAAAA_key1_end", 1);
    try t.put("AAAAAAAA_key2_end", 2);
    try t.put("AAAAAAAA_key3_end", 3);

    try testing.expectEqual(@as(?usize, 1), t.get("AAAAAAAA_key1_end"));
    try testing.expectEqual(@as(?usize, 2), t.get("AAAAAAAA_key2_end"));
    try testing.expectEqual(@as(?usize, 3), t.get("AAAAAAAA_key3_end"));
    try testing.expectEqual(@as(usize, 3), t.len());
}
