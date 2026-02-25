//! Key-slicing utilities for the Masstree.
//!
//! Keys are arbitrary byte strings.  At each trie depth, 8 bytes are
//! extracted, zero-padded on the right when the key is shorter, and
//! interpreted as a **big-endian** `u64`.
//!
//! Big-endian encoding is critical: it guarantees that numeric ordering
//! of the resulting `u64` values matches the lexicographic ordering of
//! the original byte strings, so the B⁺ tree at each layer keeps keys
//! in the correct sorted order.

const std = @import("std");
const mem = std.mem;
const config = @import("config.zig");

/// Type alias for an 8-byte key slice stored as a `u64`.
pub const Slice = u64;

/// Extract an 8-byte slice from `key` at trie depth `depth`.
///
/// `depth` is zero-indexed and counts in units of `KEY_SLICE_LEN` (8)
/// bytes.  If the key is shorter than `(depth + 1) * 8` bytes the
/// missing bytes are zero-padded on the right before big-endian
/// conversion.
///
/// ## Examples
///
/// | key             | depth | result (conceptual)          |
/// |-----------------|-------|------------------------------|
/// | `"hello world"` |     0 | big-endian of `"hello wo"`   |
/// | `"hello world"` |     1 | big-endian of `"rld\0\0…"`   |
/// | `"hi"`          |     0 | big-endian of `"hi\0\0\0…"`  |
/// | `""`            |     0 | `0`                          |
pub fn makeSlice(key: []const u8, depth: usize) Slice {
    const start = depth * config.KEY_SLICE_LEN;
    if (start >= key.len) return 0;
    const end = @min(start + config.KEY_SLICE_LEN, key.len);
    const span = key[start..end];
    var buf: [8]u8 = [_]u8{0} ** 8;
    @memcpy(buf[0..span.len], span);
    return mem.readInt(u64, &buf, .big);
}

/// Return the tail of `key` starting from byte offset `depth * 8`.
/// Returns an empty slice when the key is shorter than that offset.
pub fn suffix(key: []const u8, depth: usize) []const u8 {
    const start = depth * config.KEY_SLICE_LEN;
    if (start >= key.len) return &[_]u8{};
    return key[start..];
}

/// How many trie layers the key spans: `⌈len / 8⌉`.
/// An empty key spans 0 layers.
pub fn sliceCount(key: []const u8) usize {
    if (key.len == 0) return 0;
    return (key.len + config.KEY_SLICE_LEN - 1) / config.KEY_SLICE_LEN;
}

// ─── Inline unit tests ──────────────────────────────────────────────────────

const testing = std.testing;

test "makeSlice: first 8 bytes" {
    const s = makeSlice("hello world", 0);
    const expected = mem.readInt(u64, "hello wo".*, .big);
    try testing.expectEqual(expected, s);
}

test "makeSlice: second slice with zero padding" {
    const s = makeSlice("hello world", 1);
    var buf: [8]u8 = [_]u8{0} ** 8;
    @memcpy(buf[0..3], "rld");
    try testing.expectEqual(mem.readInt(u64, &buf, .big), s);
}

test "makeSlice: short key padded" {
    const s = makeSlice("hi", 0);
    var buf: [8]u8 = [_]u8{0} ** 8;
    @memcpy(buf[0..2], "hi");
    try testing.expectEqual(mem.readInt(u64, &buf, .big), s);
}

test "makeSlice: empty key" {
    try testing.expectEqual(@as(Slice, 0), makeSlice("", 0));
}

test "makeSlice: depth beyond key length" {
    try testing.expectEqual(@as(Slice, 0), makeSlice("short", 1));
}

test "sliceCount" {
    try testing.expectEqual(@as(usize, 0), sliceCount(""));
    try testing.expectEqual(@as(usize, 1), sliceCount("a"));
    try testing.expectEqual(@as(usize, 1), sliceCount("12345678"));
    try testing.expectEqual(@as(usize, 2), sliceCount("123456789"));
    try testing.expectEqual(@as(usize, 2), sliceCount("hello world"));
}

test "suffix" {
    try testing.expectEqualStrings("rld", suffix("hello world", 1));
    try testing.expectEqualStrings("", suffix("short", 1));
    try testing.expectEqualStrings("hello world", suffix("hello world", 0));
}

test "makeSlice: lexicographic ordering preserved" {
    const a = makeSlice("apple___", 0);
    const b = makeSlice("banana__", 0);
    try testing.expect(a < b);
}
