//! Key representation for Masstree operations.
//!
//! Keys are divided into 8-byte "ikeys" for efficient comparison.
//! The `Key` struct tracks the current position during tree traversal
//! and supports shifting to descend into trie layers.

const std = @import("std");
const config = @import("config.zig");

/// Size of an ikey in bytes.
pub const IKEY_SIZE: usize = config.KEY_SLICE_LEN;

/// Maximum supported key length in bytes (32 layers x 8 bytes).
pub const MAX_KEY_LENGTH: usize = 256;

/// Special keylenx value indicating key has a suffix.
pub const KSUF_KEYLENX: u8 = 64;

/// Base keylenx value indicating a layer pointer (>= this means layer).
pub const LAYER_KEYLENX: u8 = 128;

/// A key for Masstree operations.
///
/// Holds a borrowed byte slice and tracks the current position during
/// tree traversal. Keys can be "shifted" to descend into trie layers.
///
/// ## Shift Model
///
/// Initially `shift_count = 0` and `ikey()` returns the first 8 bytes.
/// After `shift()`, the view advances by 8 bytes into the next trie layer.
///
/// ```text
/// key = "AABBCCDD-EEFFGGHH-IIJJKKLL"
///        |-- ikey --|-- suffix --|     shift_count=0
///                    |-- ikey --|     shift_count=1
/// ```
pub const Key = struct {
    /// Full key data (borrowed, not owned).
    data: []const u8,
    /// Cached big-endian ikey for the current shift position.
    ikey_val: u64,
    /// Number of 8-byte shifts applied (trie layer depth).
    shift_count: usize,
    /// Byte offset where the suffix starts (shift_count * 8 + 8).
    suffix_start: usize,

    const Self = @This();

    /// Create a key from a byte slice.
    pub fn init(data: []const u8) Self {
        std.debug.assert(data.len <= MAX_KEY_LENGTH);
        const ik = read_ikey(data, 0);
        return .{
            .data = data,
            .ikey_val = ik,
            .shift_count = 0,
            .suffix_start = IKEY_SIZE,
        };
    }

    /// Create a key from a pre-computed ikey (no data backing).
    /// Used for synthetic keys during split operations.
    pub fn from_ikey(ik: u64) Self {
        return .{
            .data = &.{},
            .ikey_val = ik,
            .shift_count = 0,
            .suffix_start = 0,
        };
    }

    /// Create a key starting at a specific suffix offset.
    /// Used when creating a key for a specific trie layer depth.
    pub fn from_suffix(data: []const u8, depth: usize) Self {
        const ik = read_ikey(data, depth * IKEY_SIZE);
        return .{
            .data = data,
            .ikey_val = ik,
            .shift_count = depth,
            .suffix_start = (depth + 1) * IKEY_SIZE,
        };
    }

    /// Get the current 8-byte ikey (big-endian u64).
    pub fn ikey(self: Self) u64 {
        return self.ikey_val;
    }

    /// Shift the key forward by one 8-byte chunk (descend into next trie layer).
    pub fn shift(self: *Self) void {
        self.shift_count += 1;
        const offset = self.shift_count * IKEY_SIZE;
        self.ikey_val = read_ikey(self.data, offset);
        self.suffix_start = offset + IKEY_SIZE;
    }

    /// Unshift by one layer (ascend to parent trie layer).
    pub fn unshift(self: *Self) void {
        std.debug.assert(self.shift_count > 0);
        self.shift_count -= 1;
        const offset = self.shift_count * IKEY_SIZE;
        self.ikey_val = read_ikey(self.data, offset);
        self.suffix_start = offset + IKEY_SIZE;
    }

    /// Reset to the original (unshifted) key.
    pub fn unshift_all(self: *Self) void {
        self.shift_count = 0;
        self.ikey_val = read_ikey(self.data, 0);
        self.suffix_start = IKEY_SIZE;
    }

    /// Compare this key's current ikey against another ikey and keylenx.
    ///
    /// This implements the Masstree key comparison logic:
    /// - First compare ikeys numerically
    /// - If equal, compare by key length (shorter keys come first)
    pub fn compare(self: Self, other_ikey: u64, keylenx: u8) std.math.Order {
        if (self.ikey_val < other_ikey) return .lt;
        if (self.ikey_val > other_ikey) return .gt;
        // ikeys are equal — compare by length
        const self_klx = self.compute_keylenx();
        if (self_klx < keylenx) return .lt;
        if (self_klx > keylenx) return .gt;
        return .eq;
    }

    /// Whether this key has remaining bytes after the current ikey.
    pub fn has_suffix(self: Self) bool {
        return self.suffix_start < self.data.len;
    }

    /// Get the suffix (bytes after the current 8-byte ikey).
    pub fn suffix(self: Self) []const u8 {
        if (self.suffix_start >= self.data.len) return &.{};
        return self.data[self.suffix_start..];
    }

    /// Current length of the key from the current shift position.
    pub fn current_len(self: Self) usize {
        const start = self.shift_count * IKEY_SIZE;
        if (start >= self.data.len) return 0;
        return self.data.len - start;
    }

    /// Full key data (regardless of shift position).
    pub fn full_data(self: Self) []const u8 {
        return self.data;
    }

    /// Compute keylenx for the current position.
    fn compute_keylenx(self: Self) u8 {
        const cl = self.current_len();
        if (cl <= IKEY_SIZE) return @intCast(cl);
        return KSUF_KEYLENX;
    }
};

// ============================================================================
//  Standalone helpers
// ============================================================================

/// Read an 8-byte ikey from data at the given byte offset.
/// Pads with zeros if not enough bytes remain.
pub fn read_ikey(data: []const u8, offset: usize) u64 {
    if (offset >= data.len) return 0;
    const remaining = data[offset..];
    if (remaining.len >= IKEY_SIZE) {
        // Read 8 bytes as big-endian u64
        return std.mem.readInt(u64, remaining[0..IKEY_SIZE], .big);
    }
    // Pad with zeros
    var buf: [IKEY_SIZE]u8 = .{0} ** IKEY_SIZE;
    @memcpy(buf[0..remaining.len], remaining);
    return std.mem.readInt(u64, &buf, .big);
}

/// Create an 8-byte big-endian slice from a u64 ikey.
pub fn make_slice(ik: u64) [IKEY_SIZE]u8 {
    var buf: [IKEY_SIZE]u8 = undefined;
    std.mem.writeInt(u64, &buf, ik, .big);
    return buf;
}

/// Extract the suffix at a given trie depth from raw key data.
pub fn suffix_at_depth(data: []const u8, depth: usize) []const u8 {
    const start = (depth + 1) * IKEY_SIZE;
    if (start >= data.len) return &.{};
    return data[start..];
}

/// Count the number of 8-byte slices needed for a key.
pub fn slice_count(key_len: usize) usize {
    return (key_len + IKEY_SIZE - 1) / IKEY_SIZE;
}

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "Key: init basic" {
    const k = Key.init("hello");
    try testing.expectEqual(@as(u64, 0x68656c6c6f000000), k.ikey());
    try testing.expectEqual(@as(usize, 0), k.shift_count);
    try testing.expect(!k.has_suffix());
}

test "Key: init 8-byte key" {
    const k = Key.init("12345678");
    try testing.expectEqual(@as(u64, 0x3132333435363738), k.ikey());
    try testing.expect(!k.has_suffix());
}

test "Key: init long key" {
    const k = Key.init("1234567890ABCDEF");
    try testing.expectEqual(@as(u64, 0x3132333435363738), k.ikey());
    try testing.expect(k.has_suffix());
    try testing.expectEqualSlices(u8, "90ABCDEF", k.suffix());
}

test "Key: shift" {
    var k = Key.init("1234567890ABCDEF");
    try testing.expectEqual(@as(u64, 0x3132333435363738), k.ikey());

    k.shift();
    try testing.expectEqual(@as(u64, 0x3930414243444546), k.ikey());
    try testing.expectEqual(@as(usize, 1), k.shift_count);
    try testing.expect(!k.has_suffix());
}

test "Key: unshift" {
    var k = Key.init("1234567890ABCDEF");
    k.shift();
    try testing.expectEqual(@as(usize, 1), k.shift_count);

    k.unshift();
    try testing.expectEqual(@as(usize, 0), k.shift_count);
    try testing.expectEqual(@as(u64, 0x3132333435363738), k.ikey());
}

test "Key: unshift_all" {
    var k = Key.init("1234567890ABCDEFGHIJKLMN");
    k.shift();
    k.shift();
    try testing.expectEqual(@as(usize, 2), k.shift_count);

    k.unshift_all();
    try testing.expectEqual(@as(usize, 0), k.shift_count);
    try testing.expectEqual(@as(u64, 0x3132333435363738), k.ikey());
}

test "Key: compare" {
    const k = Key.init("hello");
    // Same ikey
    try testing.expectEqual(std.math.Order.eq, k.compare(0x68656c6c6f000000, 5));
    // Less
    try testing.expectEqual(std.math.Order.lt, k.compare(0x78656c6c6f000000, 5));
    // Greater
    try testing.expectEqual(std.math.Order.gt, k.compare(0x58656c6c6f000000, 5));
}

test "Key: from_ikey" {
    const k = Key.from_ikey(42);
    try testing.expectEqual(@as(u64, 42), k.ikey());
    try testing.expect(!k.has_suffix());
    try testing.expectEqual(@as(usize, 0), k.current_len());
}

test "Key: from_suffix" {
    const k = Key.from_suffix("1234567890ABCDEF", 1);
    try testing.expectEqual(@as(u64, 0x3930414243444546), k.ikey());
    try testing.expectEqual(@as(usize, 1), k.shift_count);
}

test "read_ikey: basic" {
    const ik = read_ikey("ABCDEFGH", 0);
    try testing.expectEqual(@as(u64, 0x4142434445464748), ik);
}

test "read_ikey: short" {
    const ik = read_ikey("ABC", 0);
    try testing.expectEqual(@as(u64, 0x4142430000000000), ik);
}

test "read_ikey: offset" {
    const ik = read_ikey("12345678ABCDEFGH", 8);
    try testing.expectEqual(@as(u64, 0x4142434445464748), ik);
}

test "make_slice" {
    const s = make_slice(0x4142434445464748);
    try testing.expectEqualSlices(u8, "ABCDEFGH", &s);
}

test "slice_count" {
    try testing.expectEqual(@as(usize, 1), slice_count(1));
    try testing.expectEqual(@as(usize, 1), slice_count(8));
    try testing.expectEqual(@as(usize, 2), slice_count(9));
    try testing.expectEqual(@as(usize, 2), slice_count(16));
}
