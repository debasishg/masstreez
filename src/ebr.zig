//! Epoch-Based Reclamation (EBR) for safe concurrent memory management.
//!
//! ## Design
//!
//! Three-epoch scheme: a global epoch counter advances when all pinned
//! threads have observed the current epoch.  Items retired during epoch E
//! become safe to reclaim once the global epoch reaches E+2, because by
//! then every thread has crossed at least two epoch boundaries.
//!
//! ## Epoch Layout
//!
//! ```text
//!   global_epoch: u64       (monotonically increasing)
//!   bins indexed by:        epoch % 3
//!
//!   retire during epoch E:  → bins[E % 3]
//!   advance from E to E+1: → reclaim bins[(E+1) % 3]  (items from epoch E−2)
//! ```
//!
//! ## Usage
//!
//! ```zig
//! var collector = Collector.init(allocator);
//! defer collector.deinit();
//!
//! // Each thread registers once:
//! const ts = collector.register();
//!
//! // Critical section (pin/unpin):
//! var guard = collector.pin(ts);
//! defer guard.unpin();
//!
//! // Retire a node:
//! guard.defer_retire(@ptrCast(old_leaf), reclaim_leaf_fn);
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Number of retired items per bin before attempting epoch advancement.
const BATCH_THRESHOLD: usize = 128;

// ============================================================================
//  RetiredItem
// ============================================================================

/// A pointer paired with a reclamation function, waiting for safe epoch.
pub const RetiredItem = struct {
    ptr: *anyopaque,
    reclaim_fn: *const fn (*anyopaque, Allocator) void,
};

// ============================================================================
//  RetireBin — per-epoch retirement list
// ============================================================================

/// Dynamic list of retired items for one epoch slot.
const RetireBin = struct {
    items: std.ArrayList(RetiredItem),

    fn init() RetireBin {
        return .{ .items = .{} };
    }

    fn deinit(self: *RetireBin, allocator: Allocator) void {
        self.items.deinit(allocator);
    }

    fn push(self: *RetireBin, allocator: Allocator, item: RetiredItem) void {
        self.items.append(allocator, item) catch {};
    }

    fn drain(self: *RetireBin, allocator: Allocator) void {
        for (self.items.items) |item| {
            item.reclaim_fn(item.ptr, allocator);
        }
        self.items.clearRetainingCapacity();
    }

    fn len(self: *const RetireBin) usize {
        return self.items.items.len;
    }
};

// ============================================================================
//  ThreadState — per-thread EBR bookkeeping
// ============================================================================

/// Per-thread state for the epoch collector.
///
/// Each thread that participates in EBR gets one `ThreadState`, linked
/// into the collector's thread list.  Contains a local epoch snapshot
/// and three retirement bins (one per epoch mod 3).
pub const ThreadState = struct {
    /// This thread's view of the global epoch (set on pin).
    local_epoch: std.atomic.Value(u64),

    /// Whether this thread is currently inside a critical section.
    active: std.atomic.Value(bool),

    /// Retirement bins — one per epoch mod 3.
    bins: [3]RetireBin,

    /// Intrusive linked-list pointer for the collector's thread list.
    next: std.atomic.Value(?*ThreadState),

    const Self = @This();

    pub fn create(allocator: Allocator) Allocator.Error!*Self {
        const state = try allocator.create(Self);
        state.* = .{
            .local_epoch = std.atomic.Value(u64).init(0),
            .active = std.atomic.Value(bool).init(false),
            .bins = .{
                RetireBin.init(),
                RetireBin.init(),
                RetireBin.init(),
            },
            .next = std.atomic.Value(?*ThreadState).init(null),
        };
        return state;
    }

    fn destroy(self: *Self, allocator: Allocator) void {
        for (&self.bins) |*bin| {
            bin.deinit(allocator);
        }
        allocator.destroy(self);
    }
};

// ============================================================================
//  Collector — global epoch state
// ============================================================================

/// Global epoch-based reclamation collector.
///
/// Manages the global epoch counter and a linked list of per-thread states.
/// Typically one `Collector` per `MassTree` instance.
pub const Collector = struct {
    /// Global epoch counter (monotonically increasing).
    global_epoch: std.atomic.Value(u64),

    /// Head of the per-thread state linked list.
    thread_list: std.atomic.Value(?*ThreadState),

    /// Allocator used for thread states and reclamation.
    allocator: Allocator,

    const Self = @This();

    /// Create a new collector.
    pub fn init(allocator: Allocator) Self {
        return .{
            .global_epoch = std.atomic.Value(u64).init(0),
            .thread_list = std.atomic.Value(?*ThreadState).init(null),
            .allocator = allocator,
        };
    }

    /// Destroy the collector.
    ///
    /// Reclaims all pending retirements, then frees every `ThreadState`.
    /// Only safe when no thread is pinned.
    pub fn deinit(self: *Self) void {
        self.reclaim_all();

        var current = self.thread_list.load(.acquire);
        while (current) |state| {
            const next = state.next.load(.acquire);
            state.destroy(self.allocator);
            current = next;
        }
        self.thread_list.store(null, .release);
    }

    /// Register a new thread.
    ///
    /// Returns a `ThreadState` linked into the collector's list.
    /// Lock-free (uses CAS on the list head).
    pub fn register(self: *Self) Allocator.Error!*ThreadState {
        const state = try ThreadState.create(self.allocator);

        // Lock-free push to head of linked list
        while (true) {
            const head = self.thread_list.load(.acquire);
            state.next.store(head, .release);
            if (self.thread_list.cmpxchgWeak(
                head,
                state,
                .acq_rel,
                .acquire,
            ) == null) {
                return state;
            }
        }
    }

    /// Pin the given thread to the current epoch.
    ///
    /// Returns a `Guard` that must be unpinned when the critical section
    /// ends.  While pinned, no items retired before this epoch can be freed.
    pub fn pin(self: *Self, thread: *ThreadState) Guard {
        const epoch = self.global_epoch.load(.acquire);
        thread.local_epoch.store(epoch, .release);
        thread.active.store(true, .release);

        // Re-read epoch after publishing active to close the race where
        // the epoch advances between the first load and the active store.
        const epoch2 = self.global_epoch.load(.acquire);
        if (epoch2 != epoch) {
            thread.local_epoch.store(epoch2, .release);
        }

        return .{
            .collector = self,
            .thread = thread,
            .epoch = thread.local_epoch.load(.monotonic),
        };
    }

    /// Try to advance the global epoch.
    ///
    /// Checks whether every active thread has observed the current epoch.
    /// If so, bumps the epoch and reclaims items from two epochs ago.
    pub fn try_advance(self: *Self) void {
        const current = self.global_epoch.load(.acquire);

        // Verify all active threads have caught up to `current`.
        var thread = self.thread_list.load(.acquire);
        while (thread) |state| {
            if (state.active.load(.acquire)) {
                if (state.local_epoch.load(.acquire) < current) {
                    return; // This thread hasn't observed the current epoch yet.
                }
            }
            thread = state.next.load(.acquire);
        }

        // All active threads have observed `current` — try to advance.
        if (self.global_epoch.cmpxchgStrong(
            current,
            current + 1,
            .acq_rel,
            .acquire,
        ) != null) {
            return; // Another thread advanced first.
        }

        // Reclaim bin for (current + 1) % 3.
        // Items there were retired at epoch (current − 1), which is now
        // two epochs in the past — safe to free.
        if (current >= 2) {
            const reclaim_idx = (current + 1) % 3;
            var t = self.thread_list.load(.acquire);
            while (t) |state| {
                state.bins[reclaim_idx].drain(self.allocator);
                t = state.next.load(.acquire);
            }
        }
    }

    /// Force-reclaim all pending items in every bin.
    ///
    /// **Only safe when no thread is pinned** (e.g., during tree teardown).
    pub fn reclaim_all(self: *Self) void {
        var thread = self.thread_list.load(.acquire);
        while (thread) |state| {
            for (&state.bins) |*bin| {
                bin.drain(self.allocator);
            }
            thread = state.next.load(.acquire);
        }
    }
};

// ============================================================================
//  Guard — RAII-style epoch pin
// ============================================================================

/// Guard for a pinned thread.  Provides deferred retirement.
///
/// Must be unpinned (`unpin()`) when the critical section ends.
/// Failing to unpin blocks epoch advancement and causes memory to accumulate.
pub const Guard = struct {
    collector: *Collector,
    thread: *ThreadState,
    epoch: u64,

    const Self = @This();

    /// Schedule a pointer for deferred reclamation.
    ///
    /// `reclaim_fn` will be called with `(ptr, allocator)` once all
    /// threads have advanced past this epoch.
    pub fn defer_retire(
        self: *Self,
        ptr: *anyopaque,
        reclaim_fn: *const fn (*anyopaque, Allocator) void,
    ) void {
        const bin_idx = self.epoch % 3;
        self.thread.bins[bin_idx].push(self.collector.allocator, .{
            .ptr = ptr,
            .reclaim_fn = reclaim_fn,
        });

        // Try to advance if the bin is getting large.
        if (self.thread.bins[bin_idx].len() >= BATCH_THRESHOLD) {
            self.flush();
        }
    }

    /// Force a retirement-processing attempt (try to advance the epoch).
    pub fn flush(self: *Self) void {
        self.collector.try_advance();
    }

    /// Unpin this thread, allowing epoch advancement.
    pub fn unpin(self: *Self) void {
        self.thread.active.store(false, .release);
        self.collector.try_advance();
    }
};

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "EBR: Collector init and deinit" {
    var collector = Collector.init(testing.allocator);
    collector.deinit();
}

test "EBR: register thread" {
    var collector = Collector.init(testing.allocator);
    defer collector.deinit();

    const ts = try collector.register();
    try testing.expect(ts.active.load(.monotonic) == false);
    try testing.expect(collector.thread_list.load(.monotonic) == ts);
}

test "EBR: pin and unpin" {
    var collector = Collector.init(testing.allocator);
    defer collector.deinit();

    const ts = try collector.register();
    var guard = collector.pin(ts);

    try testing.expect(ts.active.load(.monotonic) == true);
    try testing.expect(guard.epoch == 0);

    guard.unpin();
    try testing.expect(ts.active.load(.monotonic) == false);
}

test "EBR: defer_retire and reclaim_all" {
    var collector = Collector.init(testing.allocator);
    defer collector.deinit();

    const ts = try collector.register();
    var guard = collector.pin(ts);

    // Allocate a dummy block and retire it.
    const ptr = try testing.allocator.create(u64);
    ptr.* = 0xDEAD;

    guard.defer_retire(@ptrCast(ptr), struct {
        fn reclaim(p: *anyopaque, alloc: Allocator) void {
            const typed: *u64 = @ptrCast(@alignCast(p));
            alloc.destroy(typed);
        }
    }.reclaim);

    guard.unpin();

    // Force reclaim.
    collector.reclaim_all();
}

test "EBR: epoch advancement" {
    var collector = Collector.init(testing.allocator);
    defer collector.deinit();

    const ts = try collector.register();

    // Pin, unpin, advance three times to reach epoch 3.
    for (0..3) |_| {
        var guard = collector.pin(ts);
        guard.unpin();
    }

    try testing.expect(collector.global_epoch.load(.monotonic) >= 2);
}

test "EBR: items reclaimed after two epoch advances" {
    var collector = Collector.init(testing.allocator);
    defer collector.deinit();

    const ts = try collector.register();
    var reclaimed: bool = false;

    // Retire during epoch 0.
    {
        var guard = collector.pin(ts);
        guard.defer_retire(@ptrCast(&reclaimed), struct {
            fn reclaim(p: *anyopaque, _: Allocator) void {
                const flag: *bool = @ptrCast(@alignCast(p));
                flag.* = true;
            }
        }.reclaim);
        guard.unpin();
    }

    // Advance twice more so that epoch-0 items can be reclaimed.
    for (0..3) |_| {
        var guard = collector.pin(ts);
        guard.unpin();
    }

    try testing.expect(reclaimed);
}

test "EBR: multiple threads register" {
    var collector = Collector.init(testing.allocator);
    defer collector.deinit();

    const ts1 = try collector.register();
    const ts2 = try collector.register();
    const ts3 = try collector.register();

    // All three should be in the linked list.
    var count: usize = 0;
    var cur = collector.thread_list.load(.monotonic);
    while (cur) |state| {
        count += 1;
        cur = state.next.load(.monotonic);
    }
    try testing.expectEqual(@as(usize, 3), count);
    _ = ts1;
    _ = ts2;
    _ = ts3;
}

test "EBR: pinned thread blocks reclamation" {
    var collector = Collector.init(testing.allocator);
    defer collector.deinit();

    const ts1 = try collector.register();
    const ts2 = try collector.register();

    var reclaimed: bool = false;

    // ts1 retires an item at epoch 0.
    {
        var guard = collector.pin(ts1);
        guard.defer_retire(@ptrCast(&reclaimed), struct {
            fn reclaim(p: *anyopaque, _: Allocator) void {
                const flag: *bool = @ptrCast(@alignCast(p));
                flag.* = true;
            }
        }.reclaim);
        guard.unpin();
    }

    // ts2 pins and stays pinned at epoch 0.
    var guard2 = collector.pin(ts2);

    // Advance attempts — should be blocked by ts2 being pinned at epoch 0.
    for (0..5) |_| {
        var g = collector.pin(ts1);
        g.unpin();
    }

    // Epoch cannot advance past ts2's pinned epoch, so item not reclaimed.
    // (ts2 is still pinned, preventing advancement.)
    try testing.expect(!reclaimed);

    // Now unpin ts2 — epoch can advance.
    guard2.unpin();

    // Advance enough to reclaim.
    for (0..5) |_| {
        var g = collector.pin(ts1);
        g.unpin();
    }

    try testing.expect(reclaimed);
}
