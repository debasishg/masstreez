//! Extended tests for the key-slicing utilities in `src/key.zig`.

const std = @import("std");
const testing = std.testing;
const mem = std.mem;
const key = @import("masstree").key;

// ── make_slice ────────────────────────────────────────────────────────────────

test "exactly 8 bytes fills one slice perfectly" {
    const s = key.make_slice("12345678", 0);
    const expected = mem.readInt(u64, "12345678", .big);
    try testing.expectEqual(expected, s);
}

test "exactly 16 bytes — two full slices" {
    const k = "ABCDEFGHijklmnop";
    const s0 = key.make_slice(k, 0);
    const s1 = key.make_slice(k, 1);
    try testing.expectEqual(mem.readInt(u64, "ABCDEFGH", .big), s0);
    try testing.expectEqual(mem.readInt(u64, "ijklmnop", .big), s1);
}

test "depth far beyond key returns zero" {
    try testing.expectEqual(@as(key.Slice, 0), key.make_slice("abc", 100));
}

test "single byte 0xFF" {
    const s = key.make_slice(&[_]u8{0xFF}, 0);
    var buf: [8]u8 = [_]u8{0} ** 8;
    buf[0] = 0xFF;
    try testing.expectEqual(mem.readInt(u64, &buf, .big), s);
}

test "ordering: all printable ASCII single-char keys" {
    var prev: key.Slice = 0;
    for (1..128) |c| {
        const k = [_]u8{@intCast(c)};
        const s = key.make_slice(&k, 0);
        try testing.expect(s > prev);
        prev = s;
    }
}

test "ordering: multi-byte keys sorted correctly" {
    const a = key.make_slice("aaaa", 0);
    const b = key.make_slice("aaab", 0);
    const c = key.make_slice("aaba", 0);
    try testing.expect(a < b);
    try testing.expect(b < c);
}

// ── slice_count ───────────────────────────────────────────────────────────────

test "slice_count: boundary lengths" {
    try testing.expectEqual(@as(usize, 0), key.slice_count(""));
    try testing.expectEqual(@as(usize, 1), key.slice_count("1234567")); // 7
    try testing.expectEqual(@as(usize, 1), key.slice_count("12345678")); // 8
    try testing.expectEqual(@as(usize, 2), key.slice_count("123456789")); // 9
    try testing.expectEqual(@as(usize, 3), key.slice_count("12345678901234567")); // 17
}

// ── suffix ───────────────────────────────────────────────────────────────────

test "suffix: boundary cases" {
    try testing.expectEqualStrings("12345678", key.suffix("12345678", 0));
    try testing.expectEqualStrings("", key.suffix("12345678", 1));
    try testing.expectEqualStrings("9", key.suffix("123456789", 1));
}
