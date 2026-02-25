//! OCC version word for Masstree nodes.
//!
//! ## Phase 4 — Concurrent Atomic Implementation
//!
//! The `NodeVersion` tracks lock state, dirty flags, and version counters
//! in a single u32 word. All access uses Zig atomic builtins
//! (`@atomicLoad`, `@atomicStore`, `@cmpxchgWeak`) for thread safety.
//!
//! ## Bit Layout (u32)
//!
//! ```text
//! Bit  0    : LOCK        — exclusive lock (CAS spinlock)
//! Bit  1    : INSERTING   — insert in progress (dirty flag)
//! Bit  2    : SPLITTING   — split in progress (dirty flag)
//! Bits 3-5  : unused
//! Bits 6-15 : VINSERT     — insert version counter (10 bits)
//! Bits 16-27: VSPLIT      — split version counter (12 bits)
//! Bit  28   : DELETED     — node logically deleted
//! Bit  29   : ROOT        — node is a tree/layer root
//! Bit  30   : ISLEAF      — leaf (vs internode)
//! Bit  31   : unused
//! ```
//!
//! ## Synchronization Model
//!
//! Readers use optimistic concurrency control (OCC):
//! 1. `stable()` — spin until dirty bits clear, Acquire load returns snapshot
//! 2. Read node fields (ordered after the Acquire load from stable())
//! 3. `has_changed(snapshot)` — Acquire load, detect version change
//!
//! Writers use a CAS spinlock:
//! 1. `lock()` — CAS on LOCK_BIT with Acquire ordering
//! 2. `mark_insert()` / `mark_split()` — set dirty bits with SeqCst ordering
//! 3. Modify node fields (under lock exclusion)
//! 4. `release()` — increment version counter, clear dirty/lock with Release

const std = @import("std");

/// OCC version word with atomic access.
///
/// Combines lock state, dirty bits, version counters, and node metadata
/// in a single u32. Used for optimistic concurrency control reads.
pub const NodeVersion = struct {
    /// The version word. ALL access must go through atomic builtins.
    value: u32,

    const Self = @This();

    // ========================================================================
    //  Bit positions and masks
    // ========================================================================

    pub const LOCK_BIT: u32 = 1 << 0;
    pub const INSERTING_BIT: u32 = 1 << 1;
    pub const SPLITTING_BIT: u32 = 1 << 2;

    /// Dirty mask: inserting | splitting. Readers spin on these.
    pub const DIRTY_MASK: u32 = INSERTING_BIT | SPLITTING_BIT;

    /// Version counter low bits.
    pub const VINSERT_LOWBIT: u32 = 1 << 6; // bits 6-15
    pub const VSPLIT_LOWBIT: u32 = 1 << 16; // bits 16-27

    /// Metadata bits.
    pub const DELETED_BIT: u32 = 1 << 28;
    pub const ROOT_BIT: u32 = 1 << 29;
    pub const ISLEAF_BIT: u32 = 1 << 30;

    /// Masks for clearing bits on unlock.
    pub const SPLIT_UNLOCK_MASK: u32 = LOCK_BIT | SPLITTING_BIT | INSERTING_BIT;
    pub const UNLOCK_MASK: u32 = LOCK_BIT | INSERTING_BIT;

    // ========================================================================
    //  Construction
    // ========================================================================

    /// Create a new version word.
    pub fn init(leaf: bool) Self {
        var v: u32 = 0;
        if (leaf) v |= ISLEAF_BIT;
        return .{ .value = v };
    }

    /// Create a version word for a new split sibling.
    ///
    /// Born with LOCK_BIT | SPLITTING_BIT set, preventing other threads
    /// from accessing the node until it is fully installed and unlocked
    /// via `LockGuard.unlock_for_split()`.
    pub fn init_for_split(source_is_leaf: bool) Self {
        var v: u32 = LOCK_BIT | SPLITTING_BIT;
        if (source_is_leaf) v |= ISLEAF_BIT;
        return .{ .value = v };
    }

    // ========================================================================
    //  Flag queries (atomic loads)
    // ========================================================================

    pub fn is_leaf(self: *const Self) bool {
        return (@atomicLoad(u32, &self.value, .monotonic) & ISLEAF_BIT) != 0;
    }

    pub fn is_root(self: *const Self) bool {
        return (@atomicLoad(u32, &self.value, .monotonic) & ROOT_BIT) != 0;
    }

    pub fn is_deleted(self: *const Self) bool {
        return (@atomicLoad(u32, &self.value, .monotonic) & DELETED_BIT) != 0;
    }

    pub fn is_locked(self: *const Self) bool {
        return (@atomicLoad(u32, &self.value, .monotonic) & LOCK_BIT) != 0;
    }

    pub fn is_inserting(self: *const Self) bool {
        return (@atomicLoad(u32, &self.value, .monotonic) & INSERTING_BIT) != 0;
    }

    pub fn is_splitting(self: *const Self) bool {
        return (@atomicLoad(u32, &self.value, .monotonic) & SPLITTING_BIT) != 0;
    }

    // ========================================================================
    //  OCC operations
    // ========================================================================

    /// Get a stable (not-dirty) snapshot for OCC reads.
    ///
    /// Spins until neither INSERTING nor SPLITTING bits are set.
    /// Returns the raw u32 version word. Uses Acquire ordering so that
    /// all subsequent field reads see data written before the version
    /// was last stored with Release.
    pub fn stable(self: *const Self) u32 {
        const v = @atomicLoad(u32, &self.value, .acquire);
        if ((v & DIRTY_MASK) == 0) return v;
        return self.stable_slow();
    }

    fn stable_slow(self: *const Self) u32 {
        var spins: u32 = 0;
        while (true) {
            std.atomic.spinLoopHint();
            const v = @atomicLoad(u32, &self.value, .acquire);
            if ((v & DIRTY_MASK) == 0) return v;
            spins += 1;
            if (spins >= 64) {
                std.Thread.yield() catch {};
                spins = 0;
            }
        }
    }

    /// Check if the version has changed since the given snapshot.
    ///
    /// Uses Acquire ordering on the load to ensure all prior field reads
    /// have been ordered before this validation check. The XOR comparison
    /// ignores LOCK_BIT and INSERTING_BIT changes — only detects version
    /// counter increments, split/delete flags, etc.
    pub fn has_changed(self: *const Self, snapshot: u32) bool {
        const current = @atomicLoad(u32, &self.value, .acquire);
        return (snapshot ^ current) > (LOCK_BIT | INSERTING_BIT);
    }

    /// Check if a split has occurred since the snapshot.
    ///
    /// Compares the VSPLIT counter bits (16+) between snapshot and
    /// current version. Used for B-link tree advancement.
    pub fn has_split(self: *const Self, snapshot: u32) bool {
        const current = @atomicLoad(u32, &self.value, .acquire);
        // Compare everything from VSPLIT_LOWBIT upward (bits 16+)
        return (snapshot & ~@as(u32, VSPLIT_LOWBIT - 1)) != (current & ~@as(u32, VSPLIT_LOWBIT - 1));
    }

    // ========================================================================
    //  Locking (CAS spinlock)
    // ========================================================================

    /// Acquire the lock. Spins with backoff until successful.
    ///
    /// Returns a `LockGuard` that tracks the locked version word locally.
    /// The guard must be explicitly released via `release()`.
    pub fn lock(self: *Self) LockGuard {
        // Fast path: single CAS attempt
        const v = @atomicLoad(u32, &self.value, .monotonic);
        if ((v & LOCK_BIT) == 0) {
            if (@cmpxchgWeak(u32, &self.value, v, v | LOCK_BIT, .acquire, .monotonic) == null) {
                return .{ .version = self, .locked_value = v | LOCK_BIT };
            }
        }
        return self.lock_slow();
    }

    fn lock_slow(self: *Self) LockGuard {
        var spins: u32 = 0;
        while (true) {
            std.atomic.spinLoopHint();
            const v = @atomicLoad(u32, &self.value, .monotonic);
            if ((v & LOCK_BIT) == 0) {
                if (@cmpxchgWeak(u32, &self.value, v, v | LOCK_BIT, .acquire, .monotonic) == null) {
                    return .{ .version = self, .locked_value = v | LOCK_BIT };
                }
            }
            spins += 1;
            if (spins >= 64) {
                std.Thread.yield() catch {};
                spins = 0;
            }
        }
    }

    /// Try to acquire the lock without blocking.
    /// Returns null if already locked.
    pub fn try_lock(self: *Self) ?LockGuard {
        const v = @atomicLoad(u32, &self.value, .monotonic);
        if ((v & LOCK_BIT) != 0) return null;
        if (@cmpxchgStrong(u32, &self.value, v, v | LOCK_BIT, .acquire, .monotonic) == null) {
            return .{ .version = self, .locked_value = v | LOCK_BIT };
        }
        return null;
    }

    // ========================================================================
    //  Metadata modification (atomic read-modify-write)
    // ========================================================================

    /// Mark as root (atomic OR).
    pub fn mark_root(self: *Self) void {
        _ = @atomicRmw(u32, &self.value, .Or, ROOT_BIT, .release);
    }

    /// Clear root flag (atomic AND).
    pub fn mark_nonroot(self: *Self) void {
        _ = @atomicRmw(u32, &self.value, .And, ~ROOT_BIT, .release);
    }
};

// ============================================================================
//  LockGuard — RAII-style lock guard
// ============================================================================

/// Lock guard for modifying a node under the version lock.
///
/// Tracks a local copy of the version word (`locked_value`) which is
/// updated by `mark_insert()` / `mark_split()` and used to compute
/// the final version on `release()`.
///
/// The guard must be explicitly released. Failing to release leaks the lock.
pub const LockGuard = struct {
    version: *NodeVersion,
    locked_value: u32,

    const Self = @This();

    /// Mark that an insert is in progress.
    ///
    /// Sets INSERTING_BIT in the local copy and atomically stores it
    /// with SeqCst ordering (acts as both Release + Acquire barrier,
    /// ensuring the dirty bit is visible before subsequent field writes).
    pub fn mark_insert(self: *Self) void {
        self.locked_value |= NodeVersion.INSERTING_BIT;
        @atomicStore(u32, &self.version.value, self.locked_value, .seq_cst);
    }

    /// Mark that a split is in progress.
    pub fn mark_split(self: *Self) void {
        self.locked_value |= NodeVersion.SPLITTING_BIT;
        @atomicStore(u32, &self.version.value, self.locked_value, .seq_cst);
    }

    /// Mark the node as deleted (under lock).
    pub fn mark_deleted(self: *Self) void {
        self.locked_value |= NodeVersion.DELETED_BIT;
        @atomicStore(u32, &self.version.value, self.locked_value, .monotonic);
    }

    /// Clear the root flag (under lock).
    pub fn mark_nonroot(self: *Self) void {
        self.locked_value &= ~NodeVersion.ROOT_BIT;
        @atomicStore(u32, &self.version.value, self.locked_value, .monotonic);
    }

    /// Release the lock, incrementing the appropriate version counter.
    ///
    /// - If SPLITTING was set: increment VSPLIT, clear splitting + lock.
    /// - If INSERTING was set: increment VINSERT, clear inserting + lock.
    /// - Otherwise: just clear the lock bit.
    ///
    /// Uses Release ordering so all prior field writes become visible
    /// to readers who observe this version via Acquire.
    pub fn release(self: Self) void {
        const lv = self.locked_value;
        const new_value = if ((lv & NodeVersion.SPLITTING_BIT) != 0)
            // Split: increment VSPLIT, clear splitting + lock
            (lv + NodeVersion.VSPLIT_LOWBIT) & ~NodeVersion.SPLIT_UNLOCK_MASK
        else if ((lv & NodeVersion.INSERTING_BIT) != 0)
            // Insert: increment VINSERT, clear inserting + lock
            (lv + NodeVersion.VINSERT_LOWBIT) & ~NodeVersion.UNLOCK_MASK
        else
            // No dirty flags: just clear lock
            lv & ~NodeVersion.LOCK_BIT;

        @atomicStore(u32, &self.version.value, new_value, .release);
    }

    /// Unlock a split sibling after full installation.
    ///
    /// Uses SeqCst ordering to ensure ALL prior writes (to the sibling's
    /// fields) are globally visible before the unlock becomes visible.
    pub fn unlock_for_split(self: Self) void {
        const lv = self.locked_value;
        const new_value = (lv + NodeVersion.VSPLIT_LOWBIT) & ~NodeVersion.SPLIT_UNLOCK_MASK;
        @atomicStore(u32, &self.version.value, new_value, .seq_cst);
    }
};

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "NodeVersion: init leaf" {
    const v = NodeVersion.init(true);
    try testing.expect(v.is_leaf());
    try testing.expect(!v.is_root());
    try testing.expect(!v.is_locked());
    try testing.expect(!v.is_deleted());
}

test "NodeVersion: init non-leaf" {
    const v = NodeVersion.init(false);
    try testing.expect(!v.is_leaf());
}

test "NodeVersion: init_for_split" {
    const v = NodeVersion.init_for_split(true);
    try testing.expect(v.is_leaf());
    try testing.expect(v.is_locked());
    try testing.expect(v.is_splitting());
}

test "NodeVersion: lock and release" {
    var v = NodeVersion.init(true);
    const guard = v.lock();
    try testing.expect(v.is_locked());
    guard.release();
    try testing.expect(!v.is_locked());
}

test "NodeVersion: try_lock" {
    var v = NodeVersion.init(true);
    if (v.try_lock()) |guard| {
        try testing.expect(v.try_lock() == null); // already locked
        guard.release();
    }
    try testing.expect(v.try_lock() != null);
}

test "NodeVersion: mark_root" {
    var v = NodeVersion.init(true);
    try testing.expect(!v.is_root());
    v.mark_root();
    try testing.expect(v.is_root());
    v.mark_nonroot();
    try testing.expect(!v.is_root());
}

test "NodeVersion: insert version increment" {
    var v = NodeVersion.init(true);
    const snap = v.stable();

    var guard = v.lock();
    guard.mark_insert();
    guard.release();

    try testing.expect(v.has_changed(snap));
    try testing.expect(!v.is_inserting());
    try testing.expect(!v.is_locked());
}

test "NodeVersion: split version increment" {
    var v = NodeVersion.init(true);
    const snap = v.stable();

    var guard = v.lock();
    guard.mark_split();
    guard.release();

    try testing.expect(v.has_changed(snap));
    try testing.expect(!v.is_splitting());
    try testing.expect(!v.is_locked());
    try testing.expect(v.has_split(snap));
}

test "NodeVersion: has_changed detects changes" {
    var v = NodeVersion.init(true);
    const snap = v.stable();

    // No changes yet
    try testing.expect(!v.has_changed(snap));

    // Make a change
    var guard = v.lock();
    guard.mark_insert();
    guard.release();

    try testing.expect(v.has_changed(snap));
}

test "NodeVersion: stable returns after release" {
    var v = NodeVersion.init(true);
    var guard = v.lock();
    guard.mark_insert();
    guard.release();

    // After release, stable() returns immediately (no dirty bits).
    const snap = v.stable();
    try testing.expect(!v.has_changed(snap));
}

test "NodeVersion: unlock_for_split" {
    var v = NodeVersion.init_for_split(true);
    const snap_before = @atomicLoad(u32, &v.value, .monotonic);

    const guard = LockGuard{
        .version = &v,
        .locked_value = snap_before,
    };
    guard.unlock_for_split();

    try testing.expect(!v.is_locked());
    try testing.expect(!v.is_splitting());
}

test "NodeVersion: concurrent lock from two threads" {
    const Shared = struct {
        version: NodeVersion,
        counter: u32,
    };
    var shared = Shared{
        .version = NodeVersion.init(true),
        .counter = 0,
    };

    const thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Shared) void {
            for (0..1000) |_| {
                var guard = s.version.lock();
                s.counter += 1;
                guard.release();
            }
        }
    }.run, .{&shared});

    // Main thread also increments
    for (0..1000) |_| {
        var guard = shared.version.lock();
        shared.counter += 1;
        guard.release();
    }

    thread.join();
    try testing.expectEqual(@as(u32, 2000), shared.counter);
}
