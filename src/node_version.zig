//! OCC version word for Masstree nodes.
//!
//! The `NodeVersion` tracks lock state, dirty flags, and version counters
//! in a single u32 word, matching the C++/Rust masstree bit layout.
//!
//! ## Bit Layout (u32)
//!
//! ```text
//! Bit  0    : LOCK        — exclusive lock
//! Bit  1    : INSERTING   — insert in progress
//! Bit  2    : SPLITTING   — split in progress
//! Bits 3-5  : unused
//! Bits 6-15 : VINSERT     — insert version counter (10 bits)
//! Bits 16-30: VSPLIT      — split version counter (15 bits)
//! Bit  31   : (unused/overflow)
//!
//! Separate flag bits:
//! Bit  0 of a second group: DELETED
//! Bit  1 of a second group: ROOT
//! Bit  2 of a second group: ISLEAF
//! ```
//!
//! In this Phase 1 implementation, the version word uses non-atomic
//! u32 operations. Phase 4 will convert to `std.atomic.Value(u32)`.

const std = @import("std");

/// OCC version word.
///
/// Combines lock state, dirty bits, version counters, and node metadata
/// in a single u32. Used for optimistic concurrency control reads:
/// 1. Reader takes a snapshot via `stable()` (waits for unlock)
/// 2. Reader performs its operation
/// 3. Reader checks `has_changed(snapshot)` to validate
pub const NodeVersion = struct {
    value: u32,

    const Self = @This();

    // Bit positions and masks
    pub const LOCK_BIT: u32 = 1 << 0;
    pub const INSERTING_BIT: u32 = 1 << 1;
    pub const SPLITTING_BIT: u32 = 1 << 2;

    // Dirty mask: inserting | splitting
    pub const DIRTY_MASK: u32 = INSERTING_BIT | SPLITTING_BIT;

    // Version counter positions
    pub const VINSERT_LOWBIT: u32 = 1 << 6;  // bits 6-15
    pub const VSPLIT_LOWBIT: u32 = 1 << 16;   // bits 16-30

    // Metadata bits (stored in upper region)
    pub const DELETED_BIT: u32 = 1 << 28;
    pub const ROOT_BIT: u32 = 1 << 29;
    pub const ISLEAF_BIT: u32 = 1 << 30;

    // Unlock masks: determine which version counter to increment
    // When unlocking after a split, increment vsplit (clears splitting + lock)
    pub const SPLIT_UNLOCK_MASK: u32 = LOCK_BIT | SPLITTING_BIT;
    // When unlocking after an insert, increment vinsert (clears inserting + lock)
    pub const UNLOCK_MASK: u32 = LOCK_BIT | INSERTING_BIT;

    /// Create a new version word.
    pub fn init(leaf: bool) Self {
        var v: u32 = 0;
        if (leaf) v |= ISLEAF_BIT;
        return .{ .value = v };
    }

    // ========================================================================
    //  Flag queries
    // ========================================================================

    pub fn is_leaf(self: Self) bool {
        return (self.value & ISLEAF_BIT) != 0;
    }

    pub fn is_root(self: Self) bool {
        return (self.value & ROOT_BIT) != 0;
    }

    pub fn is_deleted(self: Self) bool {
        return (self.value & DELETED_BIT) != 0;
    }

    pub fn is_locked(self: Self) bool {
        return (self.value & LOCK_BIT) != 0;
    }

    pub fn is_inserting(self: Self) bool {
        return (self.value & INSERTING_BIT) != 0;
    }

    pub fn is_splitting(self: Self) bool {
        return (self.value & SPLITTING_BIT) != 0;
    }

    // ========================================================================
    //  OCC operations
    // ========================================================================

    /// Get a stable (unlocked) snapshot for OCC reads.
    /// In Phase 1 (single-threaded), this simply returns self.
    /// Phase 4 will spin-wait until the lock bit is clear.
    pub fn stable(self: Self) Self {
        // Single-threaded: just mask off lock bit for consistency
        return .{ .value = self.value & ~LOCK_BIT };
    }

    /// Check if the version has changed since the given snapshot.
    /// Used to validate OCC reads.
    pub fn has_changed(self: Self, snapshot: Self) bool {
        // Compare everything except the lock bit
        const mask: u32 = ~LOCK_BIT;
        return (self.value & mask) != (snapshot.value & mask);
    }

    // ========================================================================
    //  Locking
    // ========================================================================

    /// Acquire the lock. Returns a LockGuard for scoped modifications.
    /// In Phase 1 (single-threaded), this is non-blocking.
    /// Phase 4 will use atomic CAS with spin-wait.
    pub fn lock(self: *Self) LockGuard {
        std.debug.assert(!self.is_locked());
        self.value |= LOCK_BIT;
        return .{ .version = self };
    }

    /// Try to acquire the lock without blocking.
    /// Returns null if already locked.
    pub fn try_lock(self: *Self) ?LockGuard {
        if (self.is_locked()) return null;
        self.value |= LOCK_BIT;
        return .{ .version = self };
    }

    /// Mark as root.
    pub fn mark_root(self: *Self) void {
        self.value |= ROOT_BIT;
    }

    /// Clear root flag.
    pub fn mark_nonroot(self: *Self) void {
        self.value &= ~ROOT_BIT;
    }
};

/// RAII-style lock guard for modifying a node under the version lock.
///
/// The guard provides methods to mark modifications (insert, split, delete)
/// and must be explicitly released via `release()`.
pub const LockGuard = struct {
    version: *NodeVersion,

    const Self = @This();

    /// Mark that an insert is in progress.
    pub fn mark_insert(self: Self) void {
        self.version.value |= NodeVersion.INSERTING_BIT;
    }

    /// Mark that a split is in progress.
    pub fn mark_split(self: Self) void {
        self.version.value |= NodeVersion.SPLITTING_BIT;
    }

    /// Mark the node as deleted.
    pub fn mark_deleted(self: Self) void {
        self.version.value |= NodeVersion.DELETED_BIT;
    }

    /// Clear the root flag.
    pub fn mark_nonroot(self: Self) void {
        self.version.value &= ~NodeVersion.ROOT_BIT;
    }

    /// Release the lock, incrementing the appropriate version counter.
    ///
    /// If splitting was in progress: increment vsplit counter.
    /// If inserting was in progress: increment vinsert counter.
    /// Otherwise: just clear the lock bit.
    pub fn release(self: Self) void {
        const v = self.version;
        if ((v.value & NodeVersion.SPLITTING_BIT) != 0) {
            // Splitting: increment vsplit, clear splitting + lock
            v.value = (v.value + NodeVersion.VSPLIT_LOWBIT) & ~NodeVersion.SPLIT_UNLOCK_MASK;
        } else if ((v.value & NodeVersion.INSERTING_BIT) != 0) {
            // Inserting: increment vinsert, clear inserting + lock
            v.value = (v.value + NodeVersion.VINSERT_LOWBIT) & ~NodeVersion.UNLOCK_MASK;
        } else {
            // No dirty flags: just clear lock
            v.value &= ~NodeVersion.LOCK_BIT;
        }
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

test "NodeVersion: lock and release" {
    var v = NodeVersion.init(true);
    const guard = v.lock();
    try testing.expect(v.is_locked());
    guard.release();
    try testing.expect(!v.is_locked());
}

test "NodeVersion: try_lock" {
    var v = NodeVersion.init(true);
    const guard = v.try_lock().?;
    try testing.expect(v.try_lock() == null); // already locked
    guard.release();
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

    const guard = v.lock();
    guard.mark_insert();
    guard.release();

    try testing.expect(v.has_changed(snap));
    try testing.expect(!v.is_inserting());
    try testing.expect(!v.is_locked());
}

test "NodeVersion: split version increment" {
    var v = NodeVersion.init(true);
    const snap = v.stable();

    const guard = v.lock();
    guard.mark_split();
    guard.release();

    try testing.expect(v.has_changed(snap));
    try testing.expect(!v.is_splitting());
    try testing.expect(!v.is_locked());
}

test "NodeVersion: has_changed detects changes" {
    var v = NodeVersion.init(true);
    const snap = v.stable();

    // No changes yet
    try testing.expect(!v.has_changed(snap));

    // Make a change
    const guard = v.lock();
    guard.mark_insert();
    guard.release();

    try testing.expect(v.has_changed(snap));
}
