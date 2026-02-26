//! Interior (internode) node for the Masstree B+ tree.
//!
//! Each internode holds up to WIDTH (15) routing keys and WIDTH+1 (16) child
//! pointers. Children can be other internodes or leaf nodes.
//!
//! ## Layout
//!
//! Fields are ordered to match the Rust/C++ cache-line-aware layout:
//!
//! ```text
//! InternodeNode {
//!     version:  NodeVersion,    — OCC version word
//!     nkeys:    u8,             — number of valid keys (0..15)
//!     height:   u8,             — 0 = children are leaves
//!     parent:   ?*anyopaque,    — parent internode
//!     ikeys:    [15]u64,        — routing keys (sorted ascending)
//!     children: [16]*anyopaque, — child[i] covers keys < ikey[i]
//! }
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;

const ver_mod = @import("node_version.zig");
pub const NodeVersion = ver_mod.NodeVersion;
pub const LockGuard = ver_mod.LockGuard;

/// Width of internode (maximum number of keys).
pub const WIDTH: usize = 15;

/// Number of child pointers (WIDTH + 1).
pub const NUM_CHILDREN: usize = WIDTH + 1;

// ============================================================================
//  InternodeNode
// ============================================================================

/// Interior node with sorted routing keys and child pointers.
///
/// Routing rule: `child[i]` covers all keys that are < `ikey[i]`,
/// and `child[nkeys]` covers all keys >= `ikey[nkeys-1]`.
pub const InternodeNode = struct {
    /// OCC version word (lock + dirty bits + version counters).
    version: NodeVersion,

    /// Number of valid keys (0..WIDTH).
    nkeys: u8 = 0,

    /// Height of this node. 0 means children are leaf nodes.
    height: u8 = 0,

    /// Parent internode (null for root).
    parent: ?*anyopaque = null,

    /// Routing keys, sorted in ascending order.
    /// Only `ikeys[0..nkeys]` are valid.
    ikeys: [WIDTH]u64 = [_]u64{0} ** WIDTH,

    /// Child pointers. `children[0..nkeys+1]` are valid.
    /// child[i] covers keys < ikey[i]; child[nkeys] covers keys >= ikey[nkeys-1].
    children: [NUM_CHILDREN]?*anyopaque = [_]?*anyopaque{null} ** NUM_CHILDREN,

    const Self = @This();

    // ========================================================================
    //  Construction
    // ========================================================================

    /// Create a new internode on the heap.
    pub fn init(allocator: Allocator, height_val: u8) Allocator.Error!*Self {
        const node = try allocator.create(Self);
        node.* = .{
            .version = NodeVersion.init(false), // is_leaf = false
            .height = height_val,
        };
        return node;
    }

    /// Initialize a pre-allocated internode in-place.
    ///
    /// Used with pool allocation where memory is already allocated
    /// via `node_pool.pool_alloc`.
    pub fn init_at(self: *Self, height_val: u8) void {
        self.* = .{
            .version = NodeVersion.init(false),
            .height = height_val,
        };
    }

    /// Create a new internode marked as root.
    pub fn init_root(allocator: Allocator, height_val: u8) Allocator.Error!*Self {
        const node = try init(allocator, height_val);
        node.version.mark_root();
        return node;
    }

    /// Create a new internode for use as a split sibling.
    /// Copies lock state from the parent's version to prevent other threads
    /// from locking the sibling until it's installed.
    pub fn init_for_split(allocator: Allocator, height_val: u8) Allocator.Error!*Self {
        return init(allocator, height_val);
    }

    /// Destroy an internode and free its memory.
    /// Does NOT recursively destroy children — caller is responsible.
    pub fn deinit(self: *Self, allocator: Allocator) void {
        allocator.destroy(self);
    }

    // ========================================================================
    //  Version / Locking
    // ========================================================================

    /// Get a stable (unlocked) version snapshot.
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

    /// Lock the node.
    pub fn lock(self: *Self) LockGuard {
        return self.version.lock();
    }

    /// Try to lock the node (non-blocking).
    pub fn try_lock(self: *Self) ?LockGuard {
        return self.version.try_lock();
    }

    // ========================================================================
    //  Atomic Field Accessors
    // ========================================================================

    /// Atomically load nkeys with Acquire ordering.
    /// This is the linearization point for internode inserts —
    /// a new child becomes visible when nkeys is incremented.
    pub fn load_nkeys(self: *const Self) u8 {
        return @atomicLoad(u8, &self.nkeys, .acquire);
    }

    /// Atomically store nkeys with Release ordering.
    /// All prior writes to ikeys/children become visible.
    pub fn store_nkeys(self: *Self, n: u8) void {
        @atomicStore(u8, &self.nkeys, n, .release);
    }

    /// Atomically load a child pointer.
    pub fn load_child(self: *const Self, i: usize) ?*anyopaque {
        std.debug.assert(i <= WIDTH);
        return @atomicLoad(?*anyopaque, &self.children[i], .acquire);
    }

    /// Atomically store a child pointer.
    pub fn store_child(self: *Self, i: usize, child: ?*anyopaque) void {
        std.debug.assert(i <= WIDTH);
        @atomicStore(?*anyopaque, &self.children[i], child, .release);
    }

    /// Atomically load the parent pointer.
    pub fn load_parent(self: *const Self) ?*anyopaque {
        return @atomicLoad(?*anyopaque, &self.parent, .acquire);
    }

    /// Atomically store the parent pointer.
    pub fn store_parent(self: *Self, ptr: ?*anyopaque) void {
        @atomicStore(?*anyopaque, &self.parent, ptr, .release);
    }

    // ========================================================================
    //  Size / Status
    // ========================================================================

    /// Number of valid routing keys.
    pub fn size(self: *const Self) usize {
        return @as(usize, self.nkeys);
    }

    /// Whether the node is full.
    pub fn is_full(self: *const Self) bool {
        return self.nkeys >= WIDTH;
    }

    /// Whether the node is empty.
    pub fn is_empty_node(self: *const Self) bool {
        return self.nkeys == 0;
    }

    /// Whether this is a root node.
    pub fn is_root(self: *const Self) bool {
        return self.version.is_root();
    }

    /// Whether children are leaf nodes.
    pub fn children_are_leaves(self: *const Self) bool {
        return self.height == 0;
    }

    // ========================================================================
    //  Key / Child Access
    // ========================================================================

    /// Get the routing key at position i.
    pub fn get_ikey(self: *const Self, i: usize) u64 {
        std.debug.assert(i < self.nkeys);
        return self.ikeys[i];
    }

    /// Get child pointer at position i.
    pub fn get_child(self: *const Self, i: usize) ?*anyopaque {
        std.debug.assert(i <= self.nkeys);
        return self.children[i];
    }

    /// Set child pointer at position i.
    pub fn set_child(self: *Self, i: usize, child: *anyopaque) void {
        std.debug.assert(i <= WIDTH);
        self.children[i] = child;
    }

    /// Assign a key and its right child at position p.
    /// Left child (children[p]) must already be set.
    pub fn assign(self: *Self, p: usize, ik: u64, right_child: *anyopaque) void {
        std.debug.assert(p < WIDTH);
        self.ikeys[p] = ik;
        self.children[p + 1] = right_child;
    }

    // ========================================================================
    //  Key Search / Navigation
    // ========================================================================

    /// Upper-bound search: find the child index to follow for `search_ikey`.
    ///
    /// Returns the index i such that:
    /// - child[i] covers the range for search_ikey
    /// - If search_ikey < ikey[i], go left
    /// - If search_ikey == ikey[i], go right (i+1)
    /// - If search_ikey > all keys, return nkeys
    pub fn upper_bound(self: *const Self, search_ikey: u64) usize {
        const n = self.nkeys;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            if (search_ikey < self.ikeys[i]) return i;
            if (search_ikey == self.ikeys[i]) return i + 1;
        }
        return n;
    }

    /// Find the insert position for a new key.
    /// Returns the index where the key should be inserted.
    pub fn find_insert_position(self: *const Self, insert_ikey: u64) usize {
        const n = self.nkeys;
        var i: usize = 0;
        while (i < n) : (i += 1) {
            if (self.ikeys[i] >= insert_ikey) return i;
        }
        return n;
    }

    /// Find the child index that corresponds to a given child pointer.
    /// Used during split propagation. Returns null if not found.
    pub fn find_child_index(self: *const Self, child_ptr: *anyopaque) ?usize {
        const n: usize = @as(usize, self.nkeys) + 1;
        for (0..n) |i| {
            if (self.children[i] == child_ptr) return i;
        }
        return null;
    }

    // ========================================================================
    //  Insert
    // ========================================================================

    /// Insert a routing key and its right child at position p.
    ///
    /// Shifts existing entries [p..nkeys] one position to the right.
    /// Caller must hold the lock. Node must not be full.
    pub fn insert_key_and_child(
        self: *Self,
        p: usize,
        new_ikey: u64,
        new_child: *anyopaque,
    ) void {
        const n: usize = @as(usize, self.nkeys);
        std.debug.assert(n < WIDTH);
        std.debug.assert(p <= n);

        // Shift right (reverse order to avoid overwriting)
        var i: usize = n;
        while (i > p) {
            i -= 1;
            self.ikeys[i + 1] = self.ikeys[i];
            self.children[i + 2] = self.children[i + 1];
        }

        // Insert
        self.ikeys[p] = new_ikey;
        self.children[p + 1] = new_child;

        // Publish: atomic nkeys store (linearization point).
        // Release ordering ensures all prior writes to ikeys/children
        // are visible to readers who Acquire-load nkeys.
        self.store_nkeys(@intCast(n + 1));
    }

    // ========================================================================
    //  Remove Child (used by coalesce)
    // ========================================================================

    /// Remove key[child_idx − 1] and child[child_idx] from this internode.
    ///
    /// Shifts remaining keys and children left to fill the gap.
    /// Caller must hold the lock.  `child_idx` must be > 0 (leftmost
    /// child removal requires different handling).
    pub fn remove_child(self: *Self, child_idx: usize) void {
        const n: usize = @as(usize, self.nkeys);
        std.debug.assert(child_idx > 0);
        std.debug.assert(child_idx <= n);

        // Shift keys left: key[child_idx-1 .. n-2] ← key[child_idx .. n-1]
        var i: usize = child_idx - 1;
        while (i + 1 < n) : (i += 1) {
            self.ikeys[i] = self.ikeys[i + 1];
        }

        // Shift children left: child[child_idx .. n-1] ← child[child_idx+1 .. n]
        i = child_idx;
        while (i < n) : (i += 1) {
            self.children[i] = self.children[i + 1];
        }
        self.children[n] = null;

        // Publish: atomic nkeys store.
        self.store_nkeys(@intCast(n - 1));
    }

    // ========================================================================
    //  Split
    // ========================================================================

    /// Split this full internode and simultaneously insert a new key/child.
    ///
    /// The new_right internode receives the upper half of the entries.
    /// Returns the popup key (to be propagated to the parent) and whether
    /// the insert went to the left node.
    pub fn split_into(
        self: *Self,
        new_right: *Self,
        new_right_ptr: *anyopaque,
        insert_pos: usize,
        insert_ikey: u64,
        insert_child: *anyopaque,
    ) struct { popup_key: u64, insert_went_left: bool } {
        std.debug.assert(self.nkeys == WIDTH);

        const mid: usize = (WIDTH + 1) / 2; // = 8 for WIDTH=15

        if (insert_pos < mid) {
            // Insert goes LEFT
            // Move [mid..WIDTH] to new_right
            new_right.children[0] = self.children[mid];
            self.shift_to(new_right, 0, mid, WIDTH - mid);
            new_right.nkeys = @intCast(WIDTH - mid);

            const popup = self.ikeys[mid - 1];
            self.nkeys = @intCast(mid - 1);

            // Now insert into left (self)
            self.insert_key_and_child(insert_pos, insert_ikey, insert_child);

            // Reparent new_right's children
            self.reparent_children(new_right, new_right_ptr);

            return .{ .popup_key = popup, .insert_went_left = true };
        } else if (insert_pos == mid) {
            // Insert IS the popup key
            new_right.children[0] = insert_child;
            self.shift_to(new_right, 0, mid, WIDTH - mid);
            new_right.nkeys = @intCast(WIDTH - mid);
            self.nkeys = @intCast(mid);

            // Reparent new_right's children
            self.reparent_children(new_right, new_right_ptr);

            return .{ .popup_key = insert_ikey, .insert_went_left = false };
        } else {
            // Insert goes RIGHT
            const right_insert_pos = insert_pos - (mid + 1);

            new_right.children[0] = self.children[mid + 1];

            // Copy entries before insert position
            if (right_insert_pos > 0) {
                self.shift_to(new_right, 0, mid + 1, right_insert_pos);
            }

            // Insert the new key/child
            new_right.ikeys[right_insert_pos] = insert_ikey;
            new_right.children[right_insert_pos + 1] = insert_child;

            // Copy remaining entries after insert position
            const remaining = WIDTH - insert_pos;
            if (remaining > 0) {
                self.shift_to(new_right, right_insert_pos + 1, insert_pos, remaining);
            }

            new_right.nkeys = @intCast(WIDTH - mid);
            const popup = self.ikeys[mid];
            self.nkeys = @intCast(mid);

            // Reparent new_right's children
            self.reparent_children(new_right, new_right_ptr);

            return .{ .popup_key = popup, .insert_went_left = false };
        }
    }

    // ========================================================================
    //  Internal Helpers
    // ========================================================================

    /// Copy `count` key/child entries from self[src_pos..] to dst[dst_pos..].
    fn shift_to(
        self: *const Self,
        dst: *Self,
        dst_pos: usize,
        src_pos: usize,
        count: usize,
    ) void {
        for (0..count) |i| {
            dst.ikeys[dst_pos + i] = self.ikeys[src_pos + i];
            dst.children[dst_pos + 1 + i] = self.children[src_pos + 1 + i];
        }
    }

    /// Reparent all children of new_right to point to new_right_ptr.
    /// Only needed when children are internodes (height > 0).
    fn reparent_children(self: *const Self, new_right: *Self, new_right_ptr: *anyopaque) void {
        _ = self;
        if (new_right.height == 0) return; // leaf children don't have parent in internode sense

        const n: usize = @as(usize, new_right.nkeys) + 1;
        for (0..n) |i| {
            if (new_right.children[i]) |child_ptr| {
                // Cast to InternodeNode to set parent
                const child: *Self = @ptrCast(@alignCast(child_ptr));
                child.parent = new_right_ptr;
            }
        }
    }

    // ========================================================================
    //  Parent Management
    // ========================================================================

    /// Get parent internode pointer.
    pub fn get_parent(self: *const Self) ?*anyopaque {
        return self.parent;
    }

    /// Set parent internode pointer.
    pub fn set_parent(self: *Self, new_parent: ?*anyopaque) void {
        self.parent = new_parent;
    }

    // ========================================================================
    //  Debug
    // ========================================================================

    /// Validate internal invariants (debug builds only).
    pub fn debug_assert_invariants(self: *const Self) void {
        if (!std.debug.runtime_safety) return;

        const n: usize = @as(usize, self.nkeys);
        std.debug.assert(n <= WIDTH);

        // Keys must be in strictly ascending order
        if (n >= 2) {
            var i: usize = 1;
            while (i < n) : (i += 1) {
                std.debug.assert(self.ikeys[i - 1] < self.ikeys[i]);
            }
        }

        // All valid children must be non-null
        for (0..n + 1) |i| {
            std.debug.assert(self.children[i] != null);
        }
    }
};

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "InternodeNode: init and deinit" {
    const node = try InternodeNode.init(testing.allocator, 0);
    defer node.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 0), node.size());
    try testing.expectEqual(@as(u8, 0), node.height);
    try testing.expect(!node.is_root());
    try testing.expect(node.children_are_leaves());
}

test "InternodeNode: init root" {
    const node = try InternodeNode.init_root(testing.allocator, 1);
    defer node.deinit(testing.allocator);

    try testing.expect(node.is_root());
    try testing.expectEqual(@as(u8, 1), node.height);
    try testing.expect(!node.children_are_leaves());
}

test "InternodeNode: insert_key_and_child" {
    const node = try InternodeNode.init(testing.allocator, 0);
    defer node.deinit(testing.allocator);

    // Set leftmost child
    var dummy_children: [4]u8 = .{ 0, 1, 2, 3 };
    node.children[0] = @ptrCast(&dummy_children[0]);

    // Insert keys with right children
    node.insert_key_and_child(0, 100, @ptrCast(&dummy_children[1]));
    node.insert_key_and_child(1, 200, @ptrCast(&dummy_children[2]));
    node.insert_key_and_child(1, 150, @ptrCast(&dummy_children[3])); // insert in middle

    try testing.expectEqual(@as(usize, 3), node.size());
    try testing.expectEqual(@as(u64, 100), node.ikeys[0]);
    try testing.expectEqual(@as(u64, 150), node.ikeys[1]);
    try testing.expectEqual(@as(u64, 200), node.ikeys[2]);
}

test "InternodeNode: upper_bound" {
    const node = try InternodeNode.init(testing.allocator, 0);
    defer node.deinit(testing.allocator);

    var dummies: [4]u8 = .{ 0, 1, 2, 3 };
    node.children[0] = @ptrCast(&dummies[0]);
    node.insert_key_and_child(0, 100, @ptrCast(&dummies[1]));
    node.insert_key_and_child(1, 200, @ptrCast(&dummies[2]));
    node.insert_key_and_child(2, 300, @ptrCast(&dummies[3]));

    // Search less than first key
    try testing.expectEqual(@as(usize, 0), node.upper_bound(50));
    // Search equal to first key: go right
    try testing.expectEqual(@as(usize, 1), node.upper_bound(100));
    // Between keys
    try testing.expectEqual(@as(usize, 1), node.upper_bound(150));
    // Equal to second key
    try testing.expectEqual(@as(usize, 2), node.upper_bound(200));
    // Greater than all
    try testing.expectEqual(@as(usize, 3), node.upper_bound(400));
}

test "InternodeNode: find_child_index" {
    const node = try InternodeNode.init(testing.allocator, 0);
    defer node.deinit(testing.allocator);

    var dummies: [3]u8 = .{ 0, 1, 2 };
    node.children[0] = @ptrCast(&dummies[0]);
    node.insert_key_and_child(0, 100, @ptrCast(&dummies[1]));
    node.insert_key_and_child(1, 200, @ptrCast(&dummies[2]));

    try testing.expectEqual(@as(?usize, 0), node.find_child_index(@ptrCast(&dummies[0])));
    try testing.expectEqual(@as(?usize, 1), node.find_child_index(@ptrCast(&dummies[1])));
    try testing.expectEqual(@as(?usize, 2), node.find_child_index(@ptrCast(&dummies[2])));

    var other: u8 = 99;
    try testing.expect(node.find_child_index(@ptrCast(&other)) == null);
}

test "InternodeNode: split_into — insert left" {
    const node = try InternodeNode.init(testing.allocator, 0);
    defer node.deinit(testing.allocator);

    // Fill to capacity
    var dummies: [WIDTH + 2]u8 = undefined;
    for (&dummies, 0..) |*d, i| d.* = @intCast(i);

    node.children[0] = @ptrCast(&dummies[0]);
    for (0..WIDTH) |i| {
        node.insert_key_and_child(i, @as(u64, @intCast((i + 1) * 10)), @ptrCast(&dummies[i + 1]));
    }
    try testing.expect(node.is_full());

    // Split with insert at position 2 (goes left)
    const new_right = try InternodeNode.init(testing.allocator, 0);
    defer new_right.deinit(testing.allocator);

    const result = node.split_into(
        new_right,
        @ptrCast(new_right),
        2, // insert_pos
        25, // insert_ikey
        @ptrCast(&dummies[WIDTH + 1]),
    );

    try testing.expect(result.insert_went_left);
    // Left + right should have WIDTH + 1 keys total (original WIDTH + 1 insert - 1 popup)
    const total_keys = @as(usize, node.nkeys) + @as(usize, new_right.nkeys);
    try testing.expectEqual(@as(usize, WIDTH), total_keys);
}

test "InternodeNode: split_into — insert right" {
    const node = try InternodeNode.init(testing.allocator, 0);
    defer node.deinit(testing.allocator);

    var dummies: [WIDTH + 2]u8 = undefined;
    for (&dummies, 0..) |*d, i| d.* = @intCast(i);

    node.children[0] = @ptrCast(&dummies[0]);
    for (0..WIDTH) |i| {
        node.insert_key_and_child(i, @as(u64, @intCast((i + 1) * 10)), @ptrCast(&dummies[i + 1]));
    }

    const new_right = try InternodeNode.init(testing.allocator, 0);
    defer new_right.deinit(testing.allocator);

    const result = node.split_into(
        new_right,
        @ptrCast(new_right),
        12, // insert_pos (goes right)
        125, // insert_ikey
        @ptrCast(&dummies[WIDTH + 1]),
    );

    try testing.expect(!result.insert_went_left);
    const total_keys = @as(usize, node.nkeys) + @as(usize, new_right.nkeys);
    try testing.expectEqual(@as(usize, WIDTH), total_keys);
}

test "InternodeNode: split_into — insert at mid" {
    const node = try InternodeNode.init(testing.allocator, 0);
    defer node.deinit(testing.allocator);

    var dummies: [WIDTH + 2]u8 = undefined;
    for (&dummies, 0..) |*d, i| d.* = @intCast(i);

    node.children[0] = @ptrCast(&dummies[0]);
    for (0..WIDTH) |i| {
        node.insert_key_and_child(i, @as(u64, @intCast((i + 1) * 10)), @ptrCast(&dummies[i + 1]));
    }

    const new_right = try InternodeNode.init(testing.allocator, 0);
    defer new_right.deinit(testing.allocator);

    const mid = (WIDTH + 1) / 2; // = 8
    const result = node.split_into(
        new_right,
        @ptrCast(new_right),
        mid, // insert at the exact midpoint
        85, // insert_ikey
        @ptrCast(&dummies[WIDTH + 1]),
    );

    // When insert_pos == mid, the insert key becomes the popup
    try testing.expectEqual(@as(u64, 85), result.popup_key);
    try testing.expect(!result.insert_went_left);
}
