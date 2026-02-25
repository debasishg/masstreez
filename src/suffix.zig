//! Suffix storage for keys longer than 8 bytes.
//!
//! When a key is longer than 8 bytes, the first 8 bytes are stored as `ikey`
//! and the remaining bytes are stored in a `SuffixBag`.
//!
//! ## Memory Layout
//!
//! SuffixBag stores per-slot metadata (offset, len) and a contiguous
//! data buffer. Suffixes are appended to the buffer; fragmentation from
//! clears is reclaimed via compaction.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Number of slots (matches leaf node WIDTH).
pub const WIDTH: usize = 15;

/// Initial capacity for suffix storage.
const INITIAL_CAPACITY: usize = 128;

/// Sentinel value indicating no suffix stored.
const EMPTY_OFFSET: u32 = std.math.maxInt(u32);

// ============================================================================
//  SlotMeta
// ============================================================================

/// Metadata for a single slot's suffix.
const SlotMeta = struct {
    /// Offset into the data buffer (EMPTY_OFFSET if no suffix).
    offset: u32 = EMPTY_OFFSET,
    /// Length of the suffix.
    len: u16 = 0,

    fn has_suffix(self: SlotMeta) bool {
        return self.offset != EMPTY_OFFSET;
    }
};

// ============================================================================
//  SuffixBag
// ============================================================================

/// Contiguous storage for key suffixes.
///
/// Each leaf node can have at most WIDTH (15) suffixes, one per slot.
/// Suffixes are stored contiguously in a growable buffer.
///
/// ## Growth Strategy
///
/// When a new suffix doesn't fit:
/// 1. Compact in-place to reclaim fragmented space
/// 2. If still not enough, grow the buffer with 2x capacity
pub const SuffixBag = struct {
    /// Per-slot metadata: (offset, length) pairs.
    slots: [WIDTH]SlotMeta = [_]SlotMeta{.{}} ** WIDTH,

    /// Contiguous suffix data buffer (allocator-managed).
    data: []u8 = &.{},

    /// Total allocated capacity of data buffer.
    capacity: usize = 0,

    /// Number of bytes currently used in data buffer.
    used: usize = 0,

    /// Cached count of slots with suffixes.
    suffix_count: u8 = 0,

    /// Allocator used for data buffer.
    allocator: Allocator = undefined,

    const Self = @This();

    /// Create a new suffix bag with initial capacity.
    pub fn init(allocator: Allocator) Allocator.Error!Self {
        const data = try allocator.alloc(u8, INITIAL_CAPACITY);
        return .{
            .data = data,
            .capacity = INITIAL_CAPACITY,
            .used = 0,
            .suffix_count = 0,
            .allocator = allocator,
        };
    }

    /// Create a new suffix bag without allocating (zero capacity).
    pub fn init_empty(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
        };
    }

    /// Free all memory.
    pub fn deinit(self: *Self) void {
        if (self.capacity > 0) {
            self.allocator.free(self.data[0..self.capacity]);
        }
        self.* = .{};
    }

    /// Return the number of slots that have suffixes.
    pub fn count(self: Self) usize {
        return @as(usize, self.suffix_count);
    }

    // ========================================================================
    //  Slot Access
    // ========================================================================

    /// Check if a slot has a suffix.
    pub fn has_suffix(self: Self, slot: usize) bool {
        std.debug.assert(slot < WIDTH);
        return self.slots[slot].has_suffix();
    }

    /// Get the suffix for a slot, or null if no suffix.
    pub fn get(self: Self, slot: usize) ?[]const u8 {
        std.debug.assert(slot < WIDTH);
        const meta = self.slots[slot];
        if (!meta.has_suffix()) return null;
        const start: usize = @as(usize, meta.offset);
        const end: usize = start + @as(usize, meta.len);
        std.debug.assert(end <= self.used);
        return self.data[start..end];
    }

    /// Get the suffix for a slot, or empty slice if no suffix.
    pub fn get_or_empty(self: Self, slot: usize) []const u8 {
        return self.get(slot) orelse &.{};
    }

    // ========================================================================
    //  Assignment
    // ========================================================================

    /// Assign a suffix to a slot with smart space management.
    pub fn assign(self: *Self, slot: usize, suf: []const u8) Allocator.Error!void {
        std.debug.assert(slot < WIDTH);
        std.debug.assert(suf.len <= std.math.maxInt(u16));

        const meta = self.slots[slot];

        // Fast Path 1: Reuse existing slot if new suffix fits
        if (meta.has_suffix() and suf.len <= @as(usize, meta.len)) {
            const start: usize = @as(usize, meta.offset);
            @memcpy(self.data[start .. start + suf.len], suf);
            self.slots[slot] = .{
                .offset = meta.offset,
                .len = @intCast(suf.len),
            };
            return;
        }

        // Fast Path 2: Append if there's room
        if (self.used + suf.len <= self.capacity) {
            if (!meta.has_suffix()) self.suffix_count += 1;
            @memcpy(self.data[self.used .. self.used + suf.len], suf);
            self.slots[slot] = .{
                .offset = @intCast(self.used),
                .len = @intCast(suf.len),
            };
            self.used += suf.len;
            return;
        }

        // Slow Path: Compact, then possibly grow
        self.compact_in_place();

        if (self.used + suf.len <= self.capacity) {
            if (!self.slots[slot].has_suffix()) self.suffix_count += 1;
            @memcpy(self.data[self.used .. self.used + suf.len], suf);
            self.slots[slot] = .{
                .offset = @intCast(self.used),
                .len = @intCast(suf.len),
            };
            self.used += suf.len;
            return;
        }

        // Must grow
        const needed = self.used + suf.len;
        const new_cap = @max(self.capacity * 2, needed);
        const new_data = try self.allocator.alloc(u8, new_cap);
        if (self.capacity > 0) {
            @memcpy(new_data[0..self.used], self.data[0..self.used]);
            self.allocator.free(self.data[0..self.capacity]);
        }
        self.data = new_data;
        self.capacity = new_cap;

        if (!self.slots[slot].has_suffix()) self.suffix_count += 1;
        @memcpy(self.data[self.used .. self.used + suf.len], suf);
        self.slots[slot] = .{
            .offset = @intCast(self.used),
            .len = @intCast(suf.len),
        };
        self.used += suf.len;
    }

    /// Clear the suffix for a slot.
    pub fn clear(self: *Self, slot: usize) void {
        std.debug.assert(slot < WIDTH);
        if (self.slots[slot].has_suffix()) {
            self.suffix_count -= 1;
        }
        self.slots[slot] = .{};
    }

    // ========================================================================
    //  Comparison Helpers
    // ========================================================================

    /// Check if a slot's suffix equals the given suffix.
    pub fn suffix_equals(self: Self, slot: usize, suf: []const u8) bool {
        const stored = self.get(slot) orelse return false;
        return std.mem.eql(u8, stored, suf);
    }

    /// Compare a slot's suffix with the given suffix.
    pub fn suffix_compare(self: Self, slot: usize, suf: []const u8) ?std.math.Order {
        const stored = self.get(slot) orelse return null;
        return std.mem.order(u8, stored, suf);
    }

    // ========================================================================
    //  Internal: Compaction
    // ========================================================================

    fn compact_in_place(self: *Self) void {
        var write_pos: usize = 0;
        for (0..WIDTH) |slot| {
            const meta = self.slots[slot];
            if (!meta.has_suffix()) continue;
            const start: usize = @as(usize, meta.offset);
            const len: usize = @as(usize, meta.len);
            if (start != write_pos) {
                std.mem.copyForwards(u8, self.data[write_pos .. write_pos + len], self.data[start .. start + len]);
            }
            self.slots[slot].offset = @intCast(write_pos);
            write_pos += len;
        }
        self.used = write_pos;
    }
};

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "SuffixBag: init and deinit" {
    var bag = try SuffixBag.init(testing.allocator);
    defer bag.deinit();
    try testing.expectEqual(@as(usize, 0), bag.count());
}

test "SuffixBag: assign and get" {
    var bag = try SuffixBag.init(testing.allocator);
    defer bag.deinit();

    try bag.assign(0, "hello");
    try testing.expectEqual(@as(usize, 1), bag.count());
    try testing.expectEqualSlices(u8, "hello", bag.get(0).?);

    try bag.assign(3, "world");
    try testing.expectEqual(@as(usize, 2), bag.count());
    try testing.expectEqualSlices(u8, "world", bag.get(3).?);
}

test "SuffixBag: clear" {
    var bag = try SuffixBag.init(testing.allocator);
    defer bag.deinit();

    try bag.assign(0, "test");
    try testing.expectEqual(@as(usize, 1), bag.count());
    bag.clear(0);
    try testing.expectEqual(@as(usize, 0), bag.count());
    try testing.expect(bag.get(0) == null);
}

test "SuffixBag: reuse slot with shorter suffix" {
    var bag = try SuffixBag.init(testing.allocator);
    defer bag.deinit();

    try bag.assign(0, "hello world");
    try bag.assign(0, "hi");
    try testing.expectEqualSlices(u8, "hi", bag.get(0).?);
    try testing.expectEqual(@as(usize, 1), bag.count());
}

test "SuffixBag: suffix_equals" {
    var bag = try SuffixBag.init(testing.allocator);
    defer bag.deinit();

    try bag.assign(0, "test");
    try testing.expect(bag.suffix_equals(0, "test"));
    try testing.expect(!bag.suffix_equals(0, "other"));
    try testing.expect(!bag.suffix_equals(1, "test"));
}

test "SuffixBag: grow on overflow" {
    var bag = try SuffixBag.init(testing.allocator);
    defer bag.deinit();

    for (0..15) |i| {
        var buf: [32]u8 = undefined;
        @memset(&buf, @intCast(i));
        try bag.assign(i, &buf);
    }
    try testing.expectEqual(@as(usize, 15), bag.count());
    for (0..15) |i| {
        const suf = bag.get(i).?;
        try testing.expectEqual(@as(usize, 32), suf.len);
        try testing.expectEqual(@as(u8, @intCast(i)), suf[0]);
    }
}

test "SuffixBag: compaction reclaims space" {
    var bag = try SuffixBag.init(testing.allocator);
    defer bag.deinit();

    try bag.assign(0, "aaaa");
    try bag.assign(1, "bbbb");
    try bag.assign(2, "cccc");
    bag.clear(1);
    try bag.assign(3, "dddd");

    try testing.expectEqual(@as(usize, 3), bag.count());
    try testing.expectEqualSlices(u8, "aaaa", bag.get(0).?);
    try testing.expect(bag.get(1) == null);
    try testing.expectEqualSlices(u8, "cccc", bag.get(2).?);
    try testing.expectEqualSlices(u8, "dddd", bag.get(3).?);
}
