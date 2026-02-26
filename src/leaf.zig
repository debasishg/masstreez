//! Leaf node for one Masstree trie layer.
//!
//! ## Layout — Structure-of-Arrays (SoA)
//!
//! The leaf node uses SoA layout with cache-line-aware field ordering,
//! matching the Rust/C++ masstree design:
//!
//! ```text
//! LeafNode(V) {
//!     version:     NodeVersion,          — OCC version word
//!     modstate:    u8,                   — lifecycle state
//!     permutation: Permuter15,           — logical sorted order -> physical slot
//!     ikeys:       [15]u64,             — first 8 bytes of each key
//!     keylenx:     [15]u8,              — key discriminator
//!     values:      [15]LeafValue(V),    — value storage
//!     suffix:      SuffixBag,           — suffix storage for keys > 8 bytes
//!     next:        ?*Self,              — next leaf in chain
//!     prev:        ?*Self,              — prev leaf in chain
//!     parent:      ?*anyopaque,         — parent internode
//! }
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;

const key_mod = @import("key.zig");
const perm_mod = @import("permuter.zig");
const ver_mod = @import("node_version.zig");
const suffix_mod = @import("suffix.zig");
const value_mod = @import("value.zig");

pub const Key = key_mod.Key;
pub const Permuter15 = perm_mod.Permuter15;
pub const NodeVersion = ver_mod.NodeVersion;
pub const LockGuard = ver_mod.LockGuard;
pub const SuffixBag = suffix_mod.SuffixBag;

/// Width of leaf node (number of key slots).
pub const WIDTH: usize = 15;

/// keylenx value indicating key has a suffix (key > 8 bytes).
pub const KSUF_KEYLENX: u8 = 64;

/// keylenx value indicating slot holds a layer pointer.
pub const LAYER_KEYLENX: u8 = 128;

/// Lifecycle states for a leaf node.
pub const ModState = struct {
    pub const INSERT: u8 = 0;
    pub const REMOVE: u8 = 1;
    pub const DELETED_LAYER: u8 = 2;
    pub const EMPTY: u8 = 3;
};

/// Match result codes for key lookup.
pub const MatchResult = struct {
    pub const EXACT: i32 = 1;
    pub const MISMATCH: i32 = 0;
    pub const LAYER: i32 = -8; // -(IKEY_SIZE)
};

// ============================================================================
//  LeafNode(V) — comptime-generic leaf node with SoA layout
// ============================================================================

/// Leaf node with Structure-of-Arrays layout.
///
/// Generic over value type V. Each leaf holds up to WIDTH (15) key-value pairs,
/// with keys managed through a permuter for O(1) logical reordering.
pub fn LeafNode(comptime V: type) type {
    const LV = value_mod.LeafValue(V);

    return struct {
        // ====================================================================
        //  Fields — ordered for cache-line efficiency
        // ====================================================================

        /// OCC version word (lock + dirty bits + version counters).
        version: NodeVersion,

        /// Lifecycle state (INSERT, REMOVE, DELETED_LAYER, EMPTY).
        modstate: u8 = ModState.INSERT,

        /// Permutation: maps logical sorted order to physical slot indices.
        /// This is the linearization point for inserts.
        permutation: Permuter15,

        /// First 8 bytes of each key, stored as big-endian u64 for comparison.
        ikeys: [WIDTH]u64 = [_]u64{0} ** WIDTH,

        /// Key length discriminator per slot:
        /// - 0..8: inline key of that length (no suffix)
        /// - 64 (KSUF_KEYLENX): key has a suffix
        /// - >= 128 (LAYER_KEYLENX): slot holds a layer pointer
        keylenx: [WIDTH]u8 = [_]u8{0} ** WIDTH,

        /// Values array — one LeafValue(V) per slot.
        values: [WIDTH]LV = [_]LV{LV.init_empty()} ** WIDTH,

        /// Suffix storage for keys longer than 8 bytes.
        suffix: SuffixBag,

        /// Next leaf in the linked list (null if last).
        next: ?*Self = null,

        /// Previous leaf in the linked list (null if first).
        prev: ?*Self = null,

        /// Parent internode (null for root leaf).
        parent: ?*anyopaque = null,

        const Self = @This();

        // ====================================================================
        //  Construction
        // ====================================================================

        /// Create a new leaf node.
        pub fn init(allocator: Allocator, make_root: bool) Allocator.Error!*Self {
            const node = try allocator.create(Self);
            node.* = .{
                .version = NodeVersion.init(true), // is_leaf = true
                .permutation = Permuter15.empty(),
                .suffix = SuffixBag.init_empty(allocator),
            };
            if (make_root) {
                node.version.mark_root();
            }
            return node;
        }

        /// Initialize a pre-allocated leaf node in-place.
        ///
        /// Used with pool allocation where memory is already allocated
        /// via `node_pool.pool_alloc`.
        pub fn init_at(self: *Self, make_root: bool, allocator: Allocator) void {
            self.* = .{
                .version = NodeVersion.init(true),
                .permutation = Permuter15.empty(),
                .suffix = SuffixBag.init_empty(allocator),
            };
            if (make_root) {
                self.version.mark_root();
            }
        }

        /// Create a new leaf as a layer root (root + null parent).
        pub fn init_layer_root(allocator: Allocator) Allocator.Error!*Self {
            const node = try init(allocator, true);
            node.parent = null;
            return node;
        }

        /// Destroy a leaf node and free its memory.
        pub fn deinit(self: *Self, allocator: Allocator) void {
            self.suffix.deinit();
            allocator.destroy(self);
        }

        // ====================================================================
        //  Version / Locking
        // ====================================================================

        /// Get a stable (unlocked) version snapshot for OCC reads.
        /// Returns the raw u32 version word.
        pub fn stable(self: *const Self) u32 {
            return self.version.stable();
        }

        /// Check if version has changed since snapshot.
        pub fn has_changed(self: *const Self, snapshot: u32) bool {
            return self.version.has_changed(snapshot);
        }

        /// Check if a split has occurred since the snapshot.
        pub fn has_split(self: *const Self, snapshot: u32) bool {
            return self.version.has_split(snapshot);
        }

        /// Lock the node for modification.
        pub fn lock(self: *Self) LockGuard {
            return self.version.lock();
        }

        /// Try to lock the node (non-blocking).
        pub fn try_lock(self: *Self) ?LockGuard {
            return self.version.try_lock();
        }

        // ====================================================================
        //  Atomic Field Accessors
        // ====================================================================

        /// Atomically load the permutation with Acquire ordering.
        /// This pairs with store_permutation (Release) to form the
        /// linearization point for concurrent inserts.
        pub fn load_permutation(self: *const Self) Permuter15 {
            return .{ .value = @atomicLoad(u64, &self.permutation.value, .acquire) };
        }

        /// Atomically store the permutation with Release ordering.
        /// This is the linearization point — all prior writes to ikeys,
        /// keylenx, values, and suffixes become visible to readers
        /// who subsequently load the permutation with Acquire.
        pub fn store_permutation(self: *Self, perm: Permuter15) void {
            @atomicStore(u64, &self.permutation.value, perm.value, .release);
        }

        /// Atomically load the next leaf pointer.
        pub fn load_next(self: *const Self) ?*Self {
            return @atomicLoad(?*Self, &self.next, .acquire);
        }

        /// Atomically store the next leaf pointer.
        pub fn store_next(self: *Self, ptr: ?*Self) void {
            @atomicStore(?*Self, &self.next, ptr, .release);
        }

        /// Atomically load the prev leaf pointer.
        pub fn load_prev(self: *const Self) ?*Self {
            return @atomicLoad(?*Self, &self.prev, .acquire);
        }

        /// Atomically store the prev leaf pointer.
        pub fn store_prev(self: *Self, ptr: ?*Self) void {
            @atomicStore(?*Self, &self.prev, ptr, .release);
        }

        /// Atomically load the parent pointer.
        pub fn load_parent(self: *const Self) ?*anyopaque {
            return @atomicLoad(?*anyopaque, &self.parent, .acquire);
        }

        /// Atomically store the parent pointer.
        pub fn store_parent(self: *Self, ptr: ?*anyopaque) void {
            @atomicStore(?*anyopaque, &self.parent, ptr, .release);
        }

        // ====================================================================
        //  Size / Status
        // ====================================================================

        /// Number of keys currently in this leaf.
        pub fn size(self: *const Self) usize {
            return self.permutation.size();
        }

        /// Whether the leaf is full.
        pub fn is_full(self: *const Self) bool {
            return self.permutation.size() >= WIDTH;
        }

        /// Whether the leaf is empty.
        pub fn is_empty_node(self: *const Self) bool {
            return self.permutation.size() == 0;
        }

        /// Whether this is a root node.
        pub fn is_root(self: *const Self) bool {
            return self.version.is_root();
        }

        /// Whether the node has been deleted.
        pub fn is_deleted(self: *const Self) bool {
            return self.version.is_deleted();
        }

        // ====================================================================
        //  Key Access
        // ====================================================================

        /// Get the ikey at a physical slot.
        pub fn get_ikey(self: *const Self, slot: usize) u64 {
            std.debug.assert(slot < WIDTH);
            return self.ikeys[slot];
        }

        /// Get the keylenx at a physical slot.
        pub fn get_keylenx(self: *const Self, slot: usize) u8 {
            std.debug.assert(slot < WIDTH);
            return self.keylenx[slot];
        }

        /// Get the logical-to-physical slot mapping at position i.
        pub fn slot_at(self: *const Self, i: usize) usize {
            return self.permutation.get(i);
        }

        /// Check if a slot has a suffix.
        pub fn has_ksuf(self: *const Self, slot: usize) bool {
            return self.keylenx[slot] == KSUF_KEYLENX;
        }

        /// Check if a slot holds a layer pointer.
        pub fn is_layer_slot(self: *const Self, slot: usize) bool {
            return self.keylenx[slot] >= LAYER_KEYLENX;
        }

        /// Get suffix at a physical slot (null if no suffix).
        pub fn ksuf(self: *const Self, slot: usize) ?[]const u8 {
            if (!self.has_ksuf(slot)) return null;
            return self.suffix.get(slot);
        }

        /// Check if a slot's suffix equals the given suffix.
        pub fn ksuf_equals(self: *const Self, slot: usize, suf: []const u8) bool {
            return self.suffix.suffix_equals(slot, suf);
        }

        /// Compare a slot's suffix with the given suffix.
        pub fn ksuf_compare(self: *const Self, slot: usize, suf: []const u8) ?std.math.Order {
            return self.suffix.suffix_compare(slot, suf);
        }

        // ====================================================================
        //  Key Lookup
        // ====================================================================

        /// Find all ikey matches — returns a bitmask of physical slots
        /// whose ikey equals `target_ikey`.
        pub fn find_ikey_matches(self: *const Self, target_ikey: u64) u32 {
            var mask: u32 = 0;
            for (0..WIDTH) |slot| {
                if (self.ikeys[slot] == target_ikey) {
                    mask |= @as(u32, 1) << @intCast(slot);
                }
            }
            return mask;
        }

        /// Determine match result for a given slot, key length indicator, and suffix.
        ///
        /// Returns:
        /// - MatchResult.LAYER (-8): slot is a layer pointer, descend
        /// - MatchResult.EXACT (1): key matches exactly
        /// - MatchResult.MISMATCH (0): ikey matches but key differs
        pub fn ksuf_match_result(
            self: *const Self,
            slot: usize,
            klx: u8,
            suf: []const u8,
        ) i32 {
            // Layer pointer — descend into sublayer
            if (klx >= LAYER_KEYLENX) return MatchResult.LAYER;

            // Key has suffix — compare suffixes
            if (klx == KSUF_KEYLENX) {
                if (self.suffix.suffix_equals(slot, suf)) {
                    return MatchResult.EXACT;
                }
                return MatchResult.MISMATCH;
            }

            // Inline key (no suffix) — suffix must also be empty
            if (suf.len == 0 and klx <= 8) {
                return MatchResult.EXACT;
            }
            return MatchResult.MISMATCH;
        }

        /// Lower bound search: find the logical position where `target_ikey`
        /// should be inserted in sorted order.
        pub fn lower_bound(self: *const Self, target_ikey: u64) usize {
            return self.permutation.lower_bound(&self.ikeys, target_ikey);
        }

        // ====================================================================
        //  Slot Assignment (used by insert)
        // ====================================================================

        /// Write a key+value into a physical slot.
        ///
        /// This sets ikey, keylenx, value, and suffix for the slot.
        pub fn assign_slot(
            self: *Self,
            slot: usize,
            ik: u64,
            klx: u8,
            val: LV,
            suf: ?[]const u8,
        ) Allocator.Error!void {
            std.debug.assert(slot < WIDTH);
            self.ikeys[slot] = ik;
            self.keylenx[slot] = klx;
            self.values[slot] = val;
            if (suf) |s| {
                try self.suffix.assign(slot, s);
            } else {
                self.suffix.clear(slot);
            }
        }

        /// Assign from a Key object: extracts ikey, builds keylenx, and stores suffix.
        pub fn assign_from_key(
            self: *Self,
            slot: usize,
            k: Key,
            val: LV,
        ) Allocator.Error!void {
            const ik = k.ikey();
            var klx: u8 = undefined;
            var suf: ?[]const u8 = null;

            if (k.has_suffix()) {
                klx = KSUF_KEYLENX;
                suf = k.suffix();
            } else {
                klx = @intCast(k.current_len());
            }

            try self.assign_slot(slot, ik, klx, val, suf);
        }

        /// Clear a physical slot.
        pub fn clear_slot(self: *Self, slot: usize) void {
            std.debug.assert(slot < WIDTH);
            self.values[slot] = LV.init_empty();
            self.keylenx[slot] = 0;
            self.suffix.clear(slot);
        }

        // ====================================================================
        //  Insert (non-splitting)
        // ====================================================================

        /// Insert a key-value pair at logical position `pos`.
        ///
        /// Caller must hold the lock. The leaf must not be full.
        /// Returns the physical slot used.
        pub fn insert_at(
            self: *Self,
            pos: usize,
            ik: u64,
            klx: u8,
            val: LV,
            suf: ?[]const u8,
        ) Allocator.Error!usize {
            std.debug.assert(!self.is_full());

            // Get a free physical slot from the back of the permutation
            var perm = self.permutation;
            const slot = perm.insert_from_back(pos);

            // Write data to the physical slot
            try self.assign_slot(slot, ik, klx, val, suf);

            // Publish: atomic store permutation (linearization point).
            // Release ordering ensures all prior writes (ikeys, keylenx,
            // values, suffix) are visible to readers who Acquire-load
            // the permutation.
            self.store_permutation(perm);
            return slot;
        }

        /// Insert using a Key object.
        pub fn insert_key(
            self: *Self,
            pos: usize,
            k: Key,
            val: LV,
        ) Allocator.Error!usize {
            const ik = k.ikey();
            var klx: u8 = undefined;
            var suf: ?[]const u8 = null;

            if (k.has_suffix()) {
                klx = KSUF_KEYLENX;
                suf = k.suffix();
            } else {
                klx = @intCast(k.current_len());
            }

            return self.insert_at(pos, ik, klx, val, suf);
        }

        // ====================================================================
        //  Split
        // ====================================================================

        /// Calculate the optimal split point.
        ///
        /// Returns the logical position at which to split, and the split ikey.
        /// Returns null if the leaf cannot be split (shouldn't happen if full).
        pub fn calculate_split_point(
            self: *const Self,
            insert_pos: usize,
            insert_ikey: u64,
        ) ?value_mod.SplitPoint {
            const n = self.size();
            if (n == 0) return null;

            // Forward-sequential: inserting at end with no next leaf
            if (insert_pos >= n and self.next == null) {
                // Check that last entry has a different ikey
                const last_slot = self.permutation.get(n - 1);
                if (self.ikeys[last_slot] != insert_ikey) {
                    return .{
                        .pos = n,
                        .split_ikey = insert_ikey,
                    };
                }
            }

            // Reverse-sequential: inserting at front with no prev leaf
            if (insert_pos == 0 and self.prev == null) {
                const first_slot = self.permutation.get(0);
                if (self.ikeys[first_slot] != insert_ikey) {
                    return .{
                        .pos = 1,
                        .split_ikey = self.ikeys[first_slot],
                    };
                }
            }

            // Default: midpoint split
            var split_pos = n / 2;

            // Adjust to keep same-ikey entries together
            const split_slot = self.permutation.get(split_pos);
            const split_ik = self.ikeys[split_slot];

            // Slide right to avoid splitting a run of equal ikeys
            while (split_pos + 1 < n) {
                const next_slot = self.permutation.get(split_pos + 1);
                if (self.ikeys[next_slot] != split_ik) break;
                split_pos += 1;
            }

            // If we went all the way right, try sliding left instead
            if (split_pos + 1 >= n) {
                split_pos = n / 2;
                while (split_pos > 0) {
                    const prev_slot = self.permutation.get(split_pos - 1);
                    if (self.ikeys[prev_slot] != split_ik) break;
                    split_pos -= 1;
                }
            }

            const final_slot = self.permutation.get(split_pos);
            return .{
                .pos = split_pos,
                .split_ikey = self.ikeys[final_slot],
            };
        }

        /// Split this leaf into self (left) and new_leaf (right).
        ///
        /// Moves entries [split_pos..size] to new_leaf.
        /// Caller must hold the lock on self and have allocated new_leaf.
        pub fn split_into(
            self: *Self,
            new_leaf: *Self,
            split_pos: usize,
        ) Allocator.Error!void {
            const n = self.size();
            std.debug.assert(split_pos <= n);

            const right_count = n - split_pos;

            // Copy entries to the right leaf
            for (0..right_count) |i| {
                const src_slot = self.permutation.get(split_pos + i);
                const dst_slot = i; // right leaf uses identity mapping

                new_leaf.ikeys[dst_slot] = self.ikeys[src_slot];
                new_leaf.keylenx[dst_slot] = self.keylenx[src_slot];
                new_leaf.values[dst_slot] = self.values[src_slot];

                // Move suffix
                if (self.suffix.has_suffix(src_slot)) {
                    const suf = self.suffix.get(src_slot).?;
                    try new_leaf.suffix.assign(dst_slot, suf);
                    self.suffix.clear(src_slot);
                }
            }

            // Set right leaf permutation: identity [0..right_count], sorted
            new_leaf.permutation = Permuter15.make_sorted(@intCast(right_count));

            // Truncate left leaf permutation
            var left_perm = self.permutation;
            left_perm.set_size(@intCast(split_pos));
            self.permutation = left_perm;

            // Link siblings
            new_leaf.next = self.next;
            new_leaf.prev = self;
            if (self.next) |next_node| {
                next_node.prev = new_leaf;
            }
            self.next = new_leaf;

            // Parent will be set by caller
        }

        /// Split and insert in one pass.
        ///
        /// Returns the insert target (left or right) and the split ikey.
        pub fn split_and_insert(
            self: *Self,
            split_pos: usize,
            new_leaf: *Self,
            insert_pos: usize,
            ik: u64,
            klx: u8,
            val: LV,
            suf: ?[]const u8,
        ) Allocator.Error!struct { split_ikey: u64, target: value_mod.InsertTarget } {
            if (insert_pos >= split_pos) {
                // Insert goes to RIGHT leaf
                try self.split_into(new_leaf, split_pos);

                const right_insert_pos = insert_pos - split_pos;
                _ = try new_leaf.insert_at(right_insert_pos, ik, klx, val, suf);

                const split_ik = new_leaf.ikeys[new_leaf.permutation.get(0)];
                return .{ .split_ikey = split_ik, .target = .right };
            } else {
                // Insert goes to LEFT leaf
                try self.split_into(new_leaf, split_pos);

                _ = try self.insert_at(insert_pos, ik, klx, val, suf);

                const split_ik = new_leaf.ikeys[new_leaf.permutation.get(0)];
                return .{ .split_ikey = split_ik, .target = .left };
            }
        }

        // ====================================================================
        //  Remove
        // ====================================================================

        /// Remove the entry at logical position `pos`.
        ///
        /// Caller must hold the lock.
        pub fn remove_at(self: *Self, pos: usize) void {
            const slot = self.permutation.get(pos);
            var perm = self.permutation;
            perm.remove(pos);
            self.permutation = perm;
            self.clear_slot(slot);
        }

        /// Remove the entry that uses physical `slot`.
        pub fn remove_slot_entry(self: *Self, slot: usize) void {
            var perm = self.permutation;
            perm.remove_slot(slot);
            self.permutation = perm;
            self.clear_slot(slot);
        }

        // ====================================================================
        //  Linked List Navigation
        // ====================================================================

        /// Unlink this leaf from the sibling chain.
        pub fn unlink(self: *Self) void {
            if (self.prev) |p| {
                p.next = self.next;
            }
            if (self.next) |n| {
                n.prev = self.prev;
            }
            self.next = null;
            self.prev = null;
        }

        // ====================================================================
        //  Layer Support
        // ====================================================================

        /// Convert a value slot to a layer pointer.
        /// `is_leaf` indicates whether the layer root is a leaf node.
        pub fn make_layer(self: *Self, slot: usize, layer_ptr: *anyopaque, is_leaf: bool) void {
            std.debug.assert(slot < WIDTH);
            self.values[slot] = LV.init_layer(layer_ptr, is_leaf);
            self.keylenx[slot] = LAYER_KEYLENX;
        }

        /// Get the layer info at a slot (pointer + is_leaf flag).
        pub fn get_layer(self: *const Self, slot: usize) ?value_mod.TaggedLayerPtr {
            if (self.keylenx[slot] >= LAYER_KEYLENX) {
                return self.values[slot].try_as_layer();
            }
            return null;
        }

        // ====================================================================
        //  Value Access
        // ====================================================================

        /// Get the value at a physical slot.
        pub fn get_value(self: *const Self, slot: usize) ?V {
            return self.values[slot].try_as_value();
        }

        /// Set the value at a physical slot (update in place).
        pub fn set_value(self: *Self, slot: usize, val: V) void {
            self.values[slot] = LV.init_value(val);
        }

        // ====================================================================
        //  Debug
        // ====================================================================

        /// Validate internal invariants (debug builds only).
        pub fn debug_assert_invariants(self: *const Self) void {
            if (!std.debug.runtime_safety) return;

            const n = self.size();
            std.debug.assert(n <= WIDTH);

            // Keys must be in non-decreasing sorted order through permutation
            if (n >= 2) {
                var i: usize = 1;
                while (i < n) : (i += 1) {
                    const prev_slot = self.permutation.get(i - 1);
                    const curr_slot = self.permutation.get(i);
                    std.debug.assert(self.ikeys[prev_slot] <= self.ikeys[curr_slot]);
                }
            }
        }
    };
}

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "LeafNode: init and deinit" {
    const Leaf = LeafNode(u64);
    const node = try Leaf.init(testing.allocator, false);
    defer node.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 0), node.size());
    try testing.expect(!node.is_root());
    try testing.expect(node.is_empty_node());
}

test "LeafNode: init root" {
    const Leaf = LeafNode(u64);
    const node = try Leaf.init(testing.allocator, true);
    defer node.deinit(testing.allocator);

    try testing.expect(node.is_root());
}

test "LeafNode: insert and lookup" {
    const Leaf = LeafNode(u64);
    const LV = value_mod.LeafValue(u64);
    const node = try Leaf.init(testing.allocator, true);
    defer node.deinit(testing.allocator);

    // Insert three keys
    _ = try node.insert_at(0, 100, 8, LV.init_value(1000), null);
    _ = try node.insert_at(0, 50, 8, LV.init_value(500), null);
    _ = try node.insert_at(2, 200, 8, LV.init_value(2000), null);

    try testing.expectEqual(@as(usize, 3), node.size());

    // Verify sorted order through permutation
    const slot0 = node.permutation.get(0);
    const slot1 = node.permutation.get(1);
    const slot2 = node.permutation.get(2);
    try testing.expectEqual(@as(u64, 50), node.ikeys[slot0]);
    try testing.expectEqual(@as(u64, 100), node.ikeys[slot1]);
    try testing.expectEqual(@as(u64, 200), node.ikeys[slot2]);

    // Verify values
    try testing.expectEqual(@as(u64, 500), node.values[slot0].as_value());
    try testing.expectEqual(@as(u64, 1000), node.values[slot1].as_value());
    try testing.expectEqual(@as(u64, 2000), node.values[slot2].as_value());
}

test "LeafNode: insert with suffix" {
    const Leaf = LeafNode(u64);
    const LV = value_mod.LeafValue(u64);
    const node = try Leaf.init(testing.allocator, true);
    defer node.deinit(testing.allocator);

    _ = try node.insert_at(0, 100, KSUF_KEYLENX, LV.init_value(42), "suffix");
    try testing.expectEqual(@as(usize, 1), node.size());

    const slot = node.permutation.get(0);
    try testing.expect(node.has_ksuf(slot));
    try testing.expect(node.ksuf_equals(slot, "suffix"));
}

test "LeafNode: remove" {
    const Leaf = LeafNode(u64);
    const LV = value_mod.LeafValue(u64);
    const node = try Leaf.init(testing.allocator, true);
    defer node.deinit(testing.allocator);

    _ = try node.insert_at(0, 100, 8, LV.init_value(1), null);
    _ = try node.insert_at(1, 200, 8, LV.init_value(2), null);
    _ = try node.insert_at(2, 300, 8, LV.init_value(3), null);

    try testing.expectEqual(@as(usize, 3), node.size());

    // Remove middle entry (logical pos 1)
    node.remove_at(1);
    try testing.expectEqual(@as(usize, 2), node.size());

    // Remaining entries should be 100, 300
    const s0 = node.permutation.get(0);
    const s1 = node.permutation.get(1);
    try testing.expectEqual(@as(u64, 100), node.ikeys[s0]);
    try testing.expectEqual(@as(u64, 300), node.ikeys[s1]);
}

test "LeafNode: split" {
    const Leaf = LeafNode(u64);
    const LV = value_mod.LeafValue(u64);
    const left = try Leaf.init(testing.allocator, true);
    defer left.deinit(testing.allocator);

    // Fill the left leaf
    for (0..10) |i| {
        const ik: u64 = @intCast((i + 1) * 10);
        _ = try left.insert_at(i, ik, 8, LV.init_value(ik * 100), null);
    }
    try testing.expectEqual(@as(usize, 10), left.size());

    // Create right leaf and split at position 5
    const right = try Leaf.init(testing.allocator, false);
    defer right.deinit(testing.allocator);

    try left.split_into(right, 5);

    // Left should have 5, right should have 5
    try testing.expectEqual(@as(usize, 5), left.size());
    try testing.expectEqual(@as(usize, 5), right.size());

    // Verify linked list
    try testing.expectEqual(right, left.next.?);
    try testing.expectEqual(left, right.prev.?);
}

test "LeafNode: find_ikey_matches" {
    const Leaf = LeafNode(u64);
    const LV = value_mod.LeafValue(u64);
    const node = try Leaf.init(testing.allocator, true);
    defer node.deinit(testing.allocator);

    _ = try node.insert_at(0, 42, 8, LV.init_value(1), null);
    _ = try node.insert_at(1, 99, 8, LV.init_value(2), null);
    _ = try node.insert_at(2, 42, KSUF_KEYLENX, LV.init_value(3), "a");

    const mask = node.find_ikey_matches(42);
    // Should find 2 matches (slots where ikey == 42)
    try testing.expectEqual(@as(u32, 2), @popCount(mask));
}

test "LeafNode: layer support" {
    const Leaf = LeafNode(u64);
    const LV = value_mod.LeafValue(u64);
    const node = try Leaf.init(testing.allocator, true);
    defer node.deinit(testing.allocator);

    _ = try node.insert_at(0, 42, 8, LV.init_value(100), null);

    // Convert to layer
    var dummy: u8 = 0;
    node.make_layer(node.permutation.get(0), @ptrCast(&dummy), true);

    const slot = node.permutation.get(0);
    try testing.expect(node.is_layer_slot(slot));
    try testing.expect(node.get_layer(slot) != null);
}

test "LeafNode: calculate_split_point" {
    const Leaf = LeafNode(u64);
    const LV = value_mod.LeafValue(u64);
    const node = try Leaf.init(testing.allocator, true);
    defer node.deinit(testing.allocator);

    // Fill with distinct keys
    for (0..WIDTH) |i| {
        const ik: u64 = @intCast((i + 1) * 10);
        _ = try node.insert_at(i, ik, 8, LV.init_value(ik), null);
    }

    const sp = node.calculate_split_point(WIDTH, 160).?;
    // Forward-sequential: should split at size
    try testing.expectEqual(@as(usize, WIDTH), sp.pos);
    try testing.expectEqual(@as(u64, 160), sp.split_ikey);
}

test "LeafNode: ksuf_match_result" {
    const Leaf = LeafNode(u64);
    const LV = value_mod.LeafValue(u64);
    const node = try Leaf.init(testing.allocator, true);
    defer node.deinit(testing.allocator);

    // Inline key (no suffix)
    _ = try node.insert_at(0, 42, 5, LV.init_value(1), null);
    const slot0 = node.permutation.get(0);
    try testing.expectEqual(MatchResult.EXACT, node.ksuf_match_result(slot0, 5, ""));
    try testing.expectEqual(MatchResult.MISMATCH, node.ksuf_match_result(slot0, 5, "extra"));

    // Key with suffix
    _ = try node.insert_at(1, 99, KSUF_KEYLENX, LV.init_value(2), "hello");
    const slot1 = node.permutation.get(1);
    try testing.expectEqual(MatchResult.EXACT, node.ksuf_match_result(slot1, KSUF_KEYLENX, "hello"));
    try testing.expectEqual(MatchResult.MISMATCH, node.ksuf_match_result(slot1, KSUF_KEYLENX, "world"));

    // Layer pointer
    try testing.expectEqual(MatchResult.LAYER, node.ksuf_match_result(slot0, LAYER_KEYLENX, ""));
}

test "LeafNode: value access" {
    const Leaf = LeafNode(u64);
    const LV = value_mod.LeafValue(u64);
    const node = try Leaf.init(testing.allocator, true);
    defer node.deinit(testing.allocator);

    _ = try node.insert_at(0, 42, 8, LV.init_value(100), null);
    const slot = node.permutation.get(0);

    try testing.expectEqual(@as(u64, 100), node.get_value(slot).?);

    node.set_value(slot, 200);
    try testing.expectEqual(@as(u64, 200), node.get_value(slot).?);
}
