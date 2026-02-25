//! Extended tests for the top-level `Masstree` API in `src/tree.zig`.

const std = @import("std");
const testing = std.testing;
const Masstree = @import("masstree").Masstree;

// ── empty tree ───────────────────────────────────────────────────────────────

test "empty tree returns null / false for everything" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    try testing.expect(t.is_empty());
    try testing.expectEqual(@as(usize, 0), t.len());
    try testing.expectEqual(@as(?usize, null), t.get("x"));
    try testing.expect(!t.remove("x"));
}

// ── single entry lifecycle ───────────────────────────────────────────────────

test "insert, update, delete single key" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    try t.put("key", 99);
    try testing.expectEqual(@as(?usize, 99), t.get("key"));
    try testing.expectEqual(@as(usize, 1), t.len());

    try t.put("key", 100);
    try testing.expectEqual(@as(?usize, 100), t.get("key"));
    try testing.expectEqual(@as(usize, 1), t.len());

    try testing.expect(t.remove("key"));
    try testing.expect(t.is_empty());
}

// ── multiple keys ────────────────────────────────────────────────────────────

test "five fruit keys" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    const words = [_][]const u8{ "apple", "banana", "cherry", "date", "elderberry" };
    for (words, 0..) |w, i| try t.put(w, i);

    try testing.expectEqual(@as(usize, words.len), t.len());
    for (words, 0..) |w, i| try testing.expectEqual(@as(?usize, i), t.get(w));
}

// ── 1000 sequential keys ────────────────────────────────────────────────────

test "1000 sequential inserts and gets" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    const n: usize = 1000;
    for (0..n) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "{d:0>8}", .{i}) catch unreachable;
        try t.put(k, i);
    }
    try testing.expectEqual(n, t.len());
    for (0..n) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "{d:0>8}", .{i}) catch unreachable;
        try testing.expectEqual(@as(?usize, i), t.get(k));
    }
}

// ── reverse order insert ─────────────────────────────────────────────────────

test "reverse order 100 keys" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    var i: usize = 100;
    while (i > 0) {
        i -= 1;
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "rev_{d:0>4}", .{i}) catch unreachable;
        try t.put(k, i);
    }
    for (0..100) |j| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "rev_{d:0>4}", .{j}) catch unreachable;
        try testing.expectEqual(@as(?usize, j), t.get(k));
    }
}

// ── long keys spanning multiple trie layers ──────────────────────────────────

test "long keys that differ only at the end" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    const k1 = "this_is_a_very_long_key_that_spans_multiple_trie_layers_aaa";
    const k2 = "this_is_a_very_long_key_that_spans_multiple_trie_layers_bbb";
    try t.put(k1, 100);
    try t.put(k2, 200);
    try testing.expectEqual(@as(?usize, 100), t.get(k1));
    try testing.expectEqual(@as(?usize, 200), t.get(k2));
}

// ── prefix keys ──────────────────────────────────────────────────────────────

test "keys that are prefixes of each other" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    try t.put("a", 1);
    try t.put("ab", 2);
    try t.put("abc", 3);
    try t.put("abcdefgh", 8);
    try t.put("abcdefghi", 9);

    try testing.expectEqual(@as(?usize, 1), t.get("a"));
    try testing.expectEqual(@as(?usize, 2), t.get("ab"));
    try testing.expectEqual(@as(?usize, 3), t.get("abc"));
    try testing.expectEqual(@as(?usize, 8), t.get("abcdefgh"));
    try testing.expectEqual(@as(?usize, 9), t.get("abcdefghi"));
    try testing.expectEqual(@as(usize, 5), t.len());
}

// ── binary keys ──────────────────────────────────────────────────────────────

test "binary keys" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    const k1 = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 1 };
    const k2 = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 2 };
    const k3 = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };

    try t.put(&k1, 1);
    try t.put(&k2, 2);
    try t.put(&k3, 3);

    try testing.expectEqual(@as(?usize, 1), t.get(&k1));
    try testing.expectEqual(@as(?usize, 2), t.get(&k2));
    try testing.expectEqual(@as(?usize, 3), t.get(&k3));
}

// ── exactly 8-byte keys ─────────────────────────────────────────────────────

test "exactly 8-byte keys" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    try t.put("12345678", 1);
    try t.put("abcdefgh", 2);
    try testing.expectEqual(@as(?usize, 1), t.get("12345678"));
    try testing.expectEqual(@as(?usize, 2), t.get("abcdefgh"));
}

// ── single-byte keys ────────────────────────────────────────────────────────

test "single byte keys" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    try t.put("a", 1);
    try t.put("b", 2);
    try t.put("z", 26);

    try testing.expectEqual(@as(?usize, 1), t.get("a"));
    try testing.expectEqual(@as(?usize, 2), t.get("b"));
    try testing.expectEqual(@as(?usize, 26), t.get("z"));
}

// ── integer keys via u64 big-endian ──────────────────────────────────────────

test "u64 big-endian encoded keys" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    for (0..50) |i| {
        var buf: [8]u8 = [_]u8{0} ** 8;
        std.mem.writeInt(u64, &buf, @as(u64, @intCast(i)), .big);
        try t.put(&buf, i);
    }
    for (0..50) |i| {
        var buf: [8]u8 = [_]u8{0} ** 8;
        std.mem.writeInt(u64, &buf, @as(u64, @intCast(i)), .big);
        try testing.expectEqual(@as(?usize, i), t.get(&buf));
    }
}

// ── delete even, verify odd ──────────────────────────────────────────────────

test "delete even indices, verify odd survivors" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    const n: usize = 100;
    for (0..n) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "del_{d:0>4}", .{i}) catch unreachable;
        try t.put(k, i);
    }

    for (0..n) |i| {
        if (i % 2 == 0) {
            var buf: [32]u8 = undefined;
            const k = std.fmt.bufPrint(&buf, "del_{d:0>4}", .{i}) catch unreachable;
            try testing.expect(t.remove(k));
        }
    }

    try testing.expectEqual(n / 2, t.len());
    for (0..n) |i| {
        var buf: [32]u8 = undefined;
        const k = std.fmt.bufPrint(&buf, "del_{d:0>4}", .{i}) catch unreachable;
        if (i % 2 == 0) {
            try testing.expectEqual(@as(?usize, null), t.get(k));
        } else {
            try testing.expectEqual(@as(?usize, i), t.get(k));
        }
    }
}
