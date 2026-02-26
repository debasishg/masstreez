//! Lock-free coalesce queue for deferred empty-leaf cleanup.
//!
//! When a leaf becomes empty after a `remove()`, it is scheduled here
//! rather than being removed inline.  A separate pass (`process_batch`)
//! pops entries and performs the multi-step cleanup:
//!
//! 1. Lock the leaf and verify it is still empty.
//! 2. Mark the leaf as deleted (version word).
//! 3. Unlink it from the B-link prev/next chain.
//! 4. Remove its child pointer from the parent internode.
//! 5. Retire the leaf via EBR for safe deferred reclamation.
//!
//! ## Queue Structure
//!
//! A Treiber stack (lock-free LIFO via atomic CAS on the head pointer).
//! Order does not matter — correctness only requires that each entry is
//! eventually processed or dropped after `MAX_REQUEUE` attempts.

const std = @import("std");
const Allocator = std.mem.Allocator;

const leaf_mod = @import("leaf.zig");
const interior_mod = @import("interior.zig");
const InternodeNode = interior_mod.InternodeNode;
const ebr_mod = @import("ebr.zig");
const node_pool = @import("node_pool.zig");

/// Maximum requeue attempts before dropping an entry.
const MAX_REQUEUE: u8 = 10;

// ============================================================================
//  CoalesceEntry — lock-free stack node
// ============================================================================

/// One entry in the coalesce queue.
///
/// Heap-allocated.  The `next` pointer forms the Treiber stack.
fn CoalesceEntry(comptime V: type) type {
    const Leaf = leaf_mod.LeafNode(V);

    return struct {
        /// The empty leaf to be cleaned up.
        leaf: *Leaf,
        /// Number of times this entry has been requeued (lock contention).
        requeue_count: u8 = 0,
        /// Intrusive next pointer for the Treiber stack.
        next: ?*@This() = null,

        const Self = @This();

        fn create(allocator: Allocator, leaf: *Leaf) Allocator.Error!*Self {
            const entry = try allocator.create(Self);
            entry.* = .{ .leaf = leaf };
            return entry;
        }
    };
}

// ============================================================================
//  CoalesceQueue(V) — lock-free Treiber stack
// ============================================================================

/// Lock-free queue of empty leaves awaiting cleanup.
pub fn CoalesceQueue(comptime V: type) type {
    const Leaf = leaf_mod.LeafNode(V);
    const Entry = CoalesceEntry(V);

    return struct {
        /// Head of the Treiber stack (atomic).
        head: std.atomic.Value(?*Entry),
        /// Allocator for creating/freeing entries.
        allocator: Allocator,

        const Self = @This();

        /// Create an empty queue.
        pub fn init(allocator: Allocator) Self {
            return .{
                .head = std.atomic.Value(?*Entry).init(null),
                .allocator = allocator,
            };
        }

        /// Drain and free any remaining entries (no reclamation of leaves).
        pub fn deinit(self: *Self) void {
            var cur = self.head.load(.acquire);
            while (cur) |entry| {
                const next = entry.next;
                self.allocator.destroy(entry);
                cur = next;
            }
            self.head.store(null, .release);
        }

        /// Schedule an empty leaf for deferred cleanup.
        ///
        /// Lock-free push (CAS on head).  Never blocks.
        pub fn schedule(self: *Self, leaf: *Leaf) void {
            const entry = Entry.create(self.allocator, leaf) catch return;
            self.push(entry);
        }

        /// Process up to `limit` entries.
        ///
        /// For each entry:
        /// - If the leaf is locked: requeue (up to `MAX_REQUEUE` times).
        /// - If the leaf is no longer empty: discard (concurrent insert refilled it).
        /// - If the leaf is the leftmost in its chain (no prev): skip.
        /// - Otherwise: mark deleted → unlink from B-link → remove from parent → retire.
        pub fn process_batch(
            self: *Self,
            limit: usize,
            guard: *ebr_mod.Guard,
        ) void {
            var processed: usize = 0;
            while (processed < limit) : (processed += 1) {
                const entry = self.pop() orelse return;
                self.try_remove_one(entry, guard);
            }
        }

        // ====================================================================
        //  Internal
        // ====================================================================

        fn push(self: *Self, entry: *Entry) void {
            while (true) {
                const old_head = self.head.load(.acquire);
                entry.next = old_head;
                if (self.head.cmpxchgWeak(old_head, entry, .acq_rel, .acquire) == null) {
                    return;
                }
            }
        }

        fn pop(self: *Self) ?*Entry {
            while (true) {
                const head = self.head.load(.acquire) orelse return null;
                const next = head.next;
                if (self.head.cmpxchgWeak(head, next, .acq_rel, .acquire) == null) {
                    head.next = null;
                    return head;
                }
            }
        }

        fn try_remove_one(self: *Self, entry: *Entry, guard: *ebr_mod.Guard) void {
            const leaf = entry.leaf;

            // Try to lock the leaf.
            var lock_guard = leaf.try_lock() orelse {
                // Leaf is locked — requeue if under limit.
                if (entry.requeue_count < MAX_REQUEUE) {
                    entry.requeue_count += 1;
                    self.push(entry);
                } else {
                    self.allocator.destroy(entry);
                }
                return;
            };

            // If leaf is no longer empty, a concurrent insert refilled it.
            if (leaf.permutation.size() > 0) {
                lock_guard.release();
                self.allocator.destroy(entry);
                return;
            }

            // Cannot remove leftmost leaf (would break invariants).
            if (leaf.load_prev() == null) {
                lock_guard.release();
                self.allocator.destroy(entry);
                return;
            }

            // ---- Point of no return: mark, unlink, remove, retire ----

            // 1. Mark leaf as deleted.
            lock_guard.mark_deleted();

            // 2. Unlink from B-link prev/next chain (atomic stores).
            const prev = leaf.load_prev();
            const next = leaf.load_next();
            if (prev) |p| p.store_next(next);
            if (next) |n| n.store_prev(prev);

            // 3. Remove from parent internode.
            remove_from_parent(leaf);

            // 4. Release the leaf lock.
            lock_guard.release();

            // 5. Retire via EBR (will be freed once safe).
            guard.defer_retire(
                @ptrCast(leaf),
                node_pool.make_leaf_reclaimer(V),
            );

            self.allocator.destroy(entry);
        }

        /// Remove a leaf's child pointer from its parent internode.
        ///
        /// Locks the parent, finds the child slot, and removes it.
        /// If the parent can't be locked or found, silently skips — the
        /// deleted leaf is still safe (it's marked deleted + unlinked).
        fn remove_from_parent(leaf: *Leaf) void {
            const parent_ptr = leaf.load_parent() orelse return;
            const parent: *InternodeNode = @ptrCast(@alignCast(parent_ptr));

            var parent_guard = parent.lock();
            defer parent_guard.release();

            const child_idx = parent.find_child_index(@ptrCast(leaf)) orelse return;

            // Cannot remove child[0] (leftmost) without more complex logic.
            if (child_idx == 0) return;

            // Remove key[child_idx - 1] and child[child_idx].
            parent.remove_child(child_idx);
        }
    };
}

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "CoalesceQueue: init and deinit" {
    var q = CoalesceQueue(u64).init(testing.allocator);
    q.deinit();
}

test "CoalesceQueue: schedule and process — non-empty leaf skipped" {
    const value_mod = @import("value.zig");
    const LV = value_mod.LeafValue(u64);
    const Leaf = leaf_mod.LeafNode(u64);

    const leaf = try Leaf.init(testing.allocator, true);
    defer leaf.deinit(testing.allocator);

    // Insert a key so the leaf is non-empty.
    _ = try leaf.insert_at(0, 42, 8, LV.init_value(100), null);

    var collector = ebr_mod.Collector.init(testing.allocator);
    defer collector.deinit();
    const ts = try collector.register();
    var guard = collector.pin(ts);
    defer guard.unpin();

    var q = CoalesceQueue(u64).init(testing.allocator);
    defer q.deinit();

    q.schedule(leaf);
    q.process_batch(10, &guard);

    // Leaf should NOT be deleted because it's non-empty.
    try testing.expect(!leaf.is_deleted());
}
