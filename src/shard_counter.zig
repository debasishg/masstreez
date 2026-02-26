//! Sharded approximate counter for concurrent `len()`.
//!
//! Each thread increments/decrements its own cache-line-padded shard,
//! avoiding contention on a single atomic counter.  `load()` sums all
//! shards for an approximate total—exact only when no concurrent
//! modifications are in flight.
//!
//! ## Design
//!
//! - **16 shards**, each 128-byte aligned (Apple M-series and Intel
//!   Xeon use 128-byte effective cache line / prefetch granularity).
//! - Thread-to-shard mapping uses `threadlocal` cached shard index
//!   computed from `std.Thread.getCurrentId()`.
//! - Individual shard values are `isize` to handle transient negative
//!   per-shard counts (decrement before corresponding increment on
//!   another shard during concurrent operations).
//! - `load()` uses `Relaxed` (`.monotonic`) ordering—callers tolerate
//!   staleness by design.

const std = @import("std");

/// Number of counter shards.  16 keeps the `load()` scan fast while
/// providing good distribution across typical thread counts (1-16).
const SHARDS: usize = 16;

/// Alignment for each shard.  128 bytes prevents false sharing on
/// both x86-64 (64B L1 lines, 128B spatial prefetcher) and AArch64
/// Apple Silicon (128B L2 lines).
const SHARD_ALIGN: usize = 128;

// ============================================================================
//  PaddedCounter — one cache-line-isolated shard
// ============================================================================

/// A single atomic counter padded to `SHARD_ALIGN` bytes.
const PaddedCounter = struct {
    value: std.atomic.Value(isize) align(SHARD_ALIGN) = std.atomic.Value(isize).init(0),

    // Padding to guarantee full isolation.  The atomic value is 8 bytes;
    // the remaining space is dead padding to fill the cache line.
    _pad: [SHARD_ALIGN - @sizeOf(std.atomic.Value(isize))]u8 = undefined,
};

// Compile-time assertions matching the Rust implementation.
comptime {
    if (@alignOf(PaddedCounter) < SHARD_ALIGN) {
        @compileError("PaddedCounter alignment is less than SHARD_ALIGN");
    }
    if (@sizeOf(PaddedCounter) < SHARD_ALIGN) {
        @compileError("PaddedCounter size is less than SHARD_ALIGN");
    }
}

// ============================================================================
//  ShardedCounter
// ============================================================================

/// A sharded approximate counter.
///
/// ```zig
/// var counter = ShardedCounter{};
/// counter.increment();
/// counter.increment();
/// counter.decrement();
/// std.debug.assert(counter.load() == 1);
/// ```
pub const ShardedCounter = struct {
    /// Per-shard counters.
    shards: [SHARDS]PaddedCounter = [_]PaddedCounter{.{}} ** SHARDS,

    const Self = @This();

    // ====================================================================
    //  Thread-to-shard mapping (cached via threadlocal)
    // ====================================================================

    /// Sentinel indicating the shard index hasn't been computed yet.
    const UNSET: usize = std.math.maxInt(usize);

    /// Cached shard index for the current thread.
    threadlocal var tls_shard: usize = UNSET;

    /// Get the current thread's shard index.
    ///
    /// The index is computed once per thread from the OS thread ID
    /// and cached in a `threadlocal` variable for subsequent calls.
    inline fn shard_index() usize {
        const cached = tls_shard;
        if (cached != UNSET) return cached;
        return compute_and_cache();
    }

    /// Cold path: compute shard index from thread ID and cache it.
    fn compute_and_cache() usize {
        @branchHint(.cold);
        const tid = std.Thread.getCurrentId();
        // Use a simple hash to distribute threads across shards.
        // FNV-1a-inspired mixing of the thread ID bits.
        const raw: u64 = @bitCast(@as(i64, @intCast(tid)));
        const mixed = raw *% 0x517cc1b727220a95; // FNV-1a 64-bit prime
        const index: usize = @intCast((mixed >> 48) ^ (mixed & 0xFFFF));
        const shard = index % SHARDS;
        tls_shard = shard;
        return shard;
    }

    // ====================================================================
    //  Public API
    // ====================================================================

    /// Increment the counter by 1.
    pub inline fn increment(self: *Self) void {
        _ = self.shards[shard_index()].value.fetchAdd(1, .monotonic);
    }

    /// Decrement the counter by 1.
    pub inline fn decrement(self: *Self) void {
        _ = self.shards[shard_index()].value.fetchSub(1, .monotonic);
    }

    /// Read the approximate total count.
    ///
    /// Sums all shards with `Relaxed` ordering.  The result is exact
    /// when no concurrent modifications are in flight, and approximately
    /// correct otherwise (bounded by the number of in-flight operations).
    ///
    /// Returns 0 if the sum is transiently negative (possible during
    /// concurrent remove + insert races across shards).
    pub fn load(self: *const Self) usize {
        var total: isize = 0;
        for (&self.shards) |*shard| {
            total += shard.value.load(.monotonic);
        }
        if (total < 0) return 0;
        return @intCast(total);
    }

    /// Reset all shards to zero.
    pub fn reset(self: *Self) void {
        for (&self.shards) |*shard| {
            shard.value.store(0, .monotonic);
        }
    }
};

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "ShardedCounter: init is zero" {
    var c = ShardedCounter{};
    try testing.expectEqual(@as(usize, 0), c.load());
}

test "ShardedCounter: single-thread increment/decrement" {
    var c = ShardedCounter{};
    c.increment();
    c.increment();
    c.increment();
    try testing.expectEqual(@as(usize, 3), c.load());

    c.decrement();
    try testing.expectEqual(@as(usize, 2), c.load());
}

test "ShardedCounter: reset" {
    var c = ShardedCounter{};
    c.increment();
    c.increment();
    c.reset();
    try testing.expectEqual(@as(usize, 0), c.load());
}

test "ShardedCounter: multi-threaded increment" {
    var c = ShardedCounter{};
    const NUM_THREADS = 4;
    const OPS_PER_THREAD = 1000;

    var threads: [NUM_THREADS]std.Thread = undefined;
    for (0..NUM_THREADS) |i| {
        threads[i] = try std.Thread.spawn(.{}, struct {
            fn work(counter: *ShardedCounter) void {
                for (0..OPS_PER_THREAD) |_| {
                    counter.increment();
                }
            }
        }.work, .{&c});
    }
    for (&threads) |*t| t.join();

    try testing.expectEqual(@as(usize, NUM_THREADS * OPS_PER_THREAD), c.load());
}

test "ShardedCounter: concurrent increment and decrement" {
    var c = ShardedCounter{};
    const NUM_THREADS = 4;
    const OPS_PER_THREAD = 500;

    // 2 incrementers, 2 decrementers — net should be 0
    var threads: [NUM_THREADS]std.Thread = undefined;
    for (0..NUM_THREADS) |i| {
        if (i < NUM_THREADS / 2) {
            threads[i] = try std.Thread.spawn(.{}, struct {
                fn work(counter: *ShardedCounter) void {
                    for (0..OPS_PER_THREAD) |_| counter.increment();
                }
            }.work, .{&c});
        } else {
            threads[i] = try std.Thread.spawn(.{}, struct {
                fn work(counter: *ShardedCounter) void {
                    for (0..OPS_PER_THREAD) |_| counter.decrement();
                }
            }.work, .{&c});
        }
    }
    for (&threads) |*t| t.join();

    try testing.expectEqual(@as(usize, 0), c.load());
}

test "ShardedCounter: PaddedCounter size and alignment" {
    try testing.expect(@alignOf(PaddedCounter) >= SHARD_ALIGN);
    try testing.expect(@sizeOf(PaddedCounter) >= SHARD_ALIGN);
}
