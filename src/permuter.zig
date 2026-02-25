//! Permutation-based slot mapping for leaf nodes.
//!
//! The permuter encodes logical-to-physical slot mappings in a single u64.
//! This enables O(1) logical reordering during inserts without physically
//! moving key/value data.
//!
//! ## Bit Layout (for WIDTH=15)
//!
//! ```text
//! [63:60] = size (0..15)
//! [59:56] = slot index for logical position 0
//! [55:52] = slot index for logical position 1
//! ...
//! [ 3: 0] = slot index for logical position 14
//! ```
//!
//! Each slot index is 4 bits wide (0..15), packed from MSB to LSB.

const std = @import("std");

/// Compute bits needed per slot index for a given WIDTH.
fn slot_bits(comptime width: usize) usize {
    // Need enough bits to represent 0..width-1
    var bits: usize = 1;
    var max: usize = 2;
    while (max < width) {
        bits += 1;
        max *= 2;
    }
    return bits;
}

/// Permuter with compile-time WIDTH.
///
/// Encodes a logical-to-physical slot mapping in a u64.
/// For WIDTH=15: 4 bits per slot, 4 bits for size = 64 bits total.
pub fn Permuter(comptime WIDTH: usize) type {
    const BITS: usize = slot_bits(WIDTH);
    const MASK: u64 = (@as(u64, 1) << BITS) - 1;

    return struct {
        value: u64,

        const Self = @This();

        /// Number of bits per slot index.
        pub const SLOT_BITS = BITS;

        /// Mask for a single slot index.
        pub const WIDTH_MASK: u64 = MASK;

        /// Initial permutation: size=0, all slots identity-mapped.
        pub const INITIAL: u64 = blk: {
            var v: u64 = 0;
            for (0..WIDTH) |i| {
                v |= @as(u64, i) << @intCast((WIDTH - 1 - i) * BITS);
            }
            break :blk v;
        };

        /// Sorted permutation for n entries: identity mapping [0..n-1].
        pub const SORTED: u64 = INITIAL;

        /// Create an empty permutation (size=0).
        pub fn empty() Self {
            return .{ .value = INITIAL };
        }

        /// Create a sorted permutation of size n.
        pub fn make_sorted(n: u64) Self {
            var v = INITIAL;
            v |= (n << @intCast(WIDTH * BITS)); // set size
            return .{ .value = v };
        }

        /// Get the number of entries.
        pub fn size(self: Self) usize {
            return @intCast(self.value >> @intCast(WIDTH * BITS));
        }

        /// Get the physical slot at logical position i.
        pub fn get(self: Self, i: usize) usize {
            std.debug.assert(i < WIDTH);
            const shift_amt: u6 = @intCast((WIDTH - 1 - i) * BITS);
            return @intCast((self.value >> shift_amt) & MASK);
        }

        /// Get the free slot at the back (at position = current size).
        pub fn back(self: Self) usize {
            const sz = self.size();
            return self.get(sz);
        }

        /// Get the free slot at an offset from the back.
        pub fn back_at_offset(self: Self, offset: usize) usize {
            const sz = self.size();
            return self.get(sz + offset);
        }

        /// Set the physical slot at logical position i.
        pub fn set(self: *Self, i: usize, slot: usize) void {
            std.debug.assert(i < WIDTH);
            const shift_amt: u6 = @intCast((WIDTH - 1 - i) * BITS);
            self.value = (self.value & ~(MASK << shift_amt)) | (@as(u64, slot) << shift_amt);
        }

        /// Set the size field.
        pub fn set_size(self: *Self, n: u64) void {
            const size_shift: u6 = @intCast(WIDTH * BITS);
            self.value = (self.value & ~(MASK << size_shift)) | (n << size_shift);
        }

        /// Insert a free slot from the back into logical position i.
        ///
        /// Shifts entries [i..size-1] right by one position.
        /// The free slot (at position `size`) moves into position i.
        /// Size is incremented.
        /// Returns the physical slot that was assigned.
        pub fn insert_from_back(self: *Self, i: usize) usize {
            const sz = self.size();
            std.debug.assert(sz < WIDTH);
            std.debug.assert(i <= sz);

            const new_slot = self.get(sz); // free slot at back

            // Shift entries [i..sz-1] right by one position
            var j: usize = sz;
            while (j > i) {
                j -= 1;
                self.set(j + 1, self.get(j));
            }

            // Place the new slot at position i
            self.set(i, new_slot);

            // Increment size
            self.set_size(@intCast(sz + 1));

            return new_slot;
        }

        /// Immutable version of insert_from_back. Returns new permuter and slot.
        pub fn insert_from_back_immutable(self: Self, i: usize) struct { perm: Self, slot: usize } {
            var copy = self;
            const slot = copy.insert_from_back(i);
            return .{ .perm = copy, .slot = slot };
        }

        /// Remove the entry at logical position i.
        ///
        /// Shifts entries [i+1..size-1] left by one position.
        /// The removed slot goes to the back (position size-1).
        /// Size is decremented.
        pub fn remove(self: *Self, i: usize) void {
            const sz = self.size();
            std.debug.assert(i < sz);

            const removed_slot = self.get(i);

            // Shift entries [i+1..sz-1] left
            var j: usize = i;
            while (j + 1 < sz) : (j += 1) {
                self.set(j, self.get(j + 1));
            }

            // Put removed slot at position sz-1 (now a free slot)
            self.set(sz - 1, removed_slot);

            // Decrement size
            self.set_size(@intCast(sz - 1));
        }

        /// Remove an entry and push the freed slot to the very back.
        pub fn remove_to_back(self: *Self, i: usize) void {
            self.remove(i);
        }

        /// Remove the entry that maps to the given physical slot.
        pub fn remove_slot(self: *Self, slot: usize) void {
            const sz = self.size();
            // Find which logical position maps to this slot
            for (0..sz) |i| {
                if (self.get(i) == slot) {
                    self.remove(i);
                    return;
                }
            }
            // Slot not found in active range — this is a programming error
            unreachable;
        }

        /// Exchange (swap) the slots at logical positions i and j.
        pub fn exchange(self: *Self, i: usize, j: usize) void {
            const si = self.get(i);
            const sj = self.get(j);
            self.set(i, sj);
            self.set(j, si);
        }

        /// Swap the first two free slots (at positions size and size+1).
        pub fn swap_free_slots(self: *Self) void {
            const sz = self.size();
            std.debug.assert(sz + 1 < WIDTH);
            const a = self.get(sz);
            const b = self.get(sz + 1);
            self.set(sz, b);
            self.set(sz + 1, a);
        }

        /// Lower-bound binary search: find the first logical position i
        /// where `ikeys[perm[i]] >= target`.
        pub fn lower_bound(self: Self, ikeys: *const [WIDTH]u64, target: u64) usize {
            const sz = self.size();
            var lo: usize = 0;
            var hi: usize = sz;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const slot = self.get(mid);
                if (ikeys[slot] < target) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            return lo;
        }

        /// Debug validation: assert all slot indices are unique and in range.
        pub fn debug_assert_valid(self: Self) void {
            if (!std.debug.runtime_safety) return;
            var seen: [WIDTH]bool = .{false} ** WIDTH;
            for (0..WIDTH) |i| {
                const slot = self.get(i);
                std.debug.assert(slot < WIDTH);
                std.debug.assert(!seen[slot]);
                seen[slot] = true;
            }
        }
    };
}

/// Standard leaf permuter with WIDTH=15.
pub const Permuter15 = Permuter(15);

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "Permuter15: empty" {
    const p = Permuter15.empty();
    try testing.expectEqual(@as(usize, 0), p.size());
}

test "Permuter15: insert_from_back" {
    var p = Permuter15.empty();
    const s0 = p.insert_from_back(0); // insert at pos 0
    try testing.expectEqual(@as(usize, 1), p.size());
    try testing.expectEqual(s0, p.get(0));

    const s1 = p.insert_from_back(0); // insert at pos 0, pushing s0 to pos 1
    try testing.expectEqual(@as(usize, 2), p.size());
    try testing.expectEqual(s1, p.get(0));
    try testing.expectEqual(s0, p.get(1));
}

test "Permuter15: remove" {
    var p = Permuter15.empty();
    _ = p.insert_from_back(0);
    _ = p.insert_from_back(1);
    _ = p.insert_from_back(2);
    try testing.expectEqual(@as(usize, 3), p.size());

    p.remove(1); // remove middle
    try testing.expectEqual(@as(usize, 2), p.size());
}

test "Permuter15: make_sorted" {
    const p = Permuter15.make_sorted(5);
    try testing.expectEqual(@as(usize, 5), p.size());
    // Identity mapping
    for (0..5) |i| {
        try testing.expectEqual(i, p.get(i));
    }
}

test "Permuter15: lower_bound" {
    var p = Permuter15.empty();
    var ikeys: [15]u64 = .{0} ** 15;

    // Insert keys 10, 20, 30 at physical slots 0, 1, 2
    const s0 = p.insert_from_back(0);
    ikeys[s0] = 10;
    const s1 = p.insert_from_back(1);
    ikeys[s1] = 20;
    const s2 = p.insert_from_back(2);
    ikeys[s2] = 30;

    try testing.expectEqual(@as(usize, 0), p.lower_bound(&ikeys, 5));
    try testing.expectEqual(@as(usize, 0), p.lower_bound(&ikeys, 10));
    try testing.expectEqual(@as(usize, 1), p.lower_bound(&ikeys, 15));
    try testing.expectEqual(@as(usize, 1), p.lower_bound(&ikeys, 20));
    try testing.expectEqual(@as(usize, 2), p.lower_bound(&ikeys, 25));
    try testing.expectEqual(@as(usize, 3), p.lower_bound(&ikeys, 35));
}

test "Permuter15: exchange" {
    var p = Permuter15.empty();
    _ = p.insert_from_back(0); // slot A at pos 0
    _ = p.insert_from_back(1); // slot B at pos 1
    const a = p.get(0);
    const b = p.get(1);
    p.exchange(0, 1);
    try testing.expectEqual(b, p.get(0));
    try testing.expectEqual(a, p.get(1));
}

test "Permuter15: remove_slot" {
    var p = Permuter15.empty();
    const s0 = p.insert_from_back(0);
    _ = p.insert_from_back(1);
    const s2 = p.insert_from_back(2);

    p.remove_slot(s0);
    try testing.expectEqual(@as(usize, 2), p.size());
    // s2 should still be in the active set
    var found = false;
    for (0..2) |i| {
        if (p.get(i) == s2) found = true;
    }
    try testing.expect(found);
}

test "Permuter15: debug_assert_valid" {
    var p = Permuter15.empty();
    _ = p.insert_from_back(0);
    _ = p.insert_from_back(1);
    p.debug_assert_valid();
}

test "Permuter15: insert_from_back_immutable" {
    const p = Permuter15.empty();
    const result = p.insert_from_back_immutable(0);
    try testing.expectEqual(@as(usize, 1), result.perm.size());
    try testing.expectEqual(@as(usize, 0), p.size()); // original unchanged
}
