//! MassTree — A trie of B+ trees for byte-string keys.
//!
//! Generic over value type `V` via Zig comptime. Each trie layer
//! consumes 8 bytes of the key as a `u64` ikey. Within a layer, entries
//! are stored in B+ tree leaf nodes with permuter-based slot management.
//!
//! ## Phase 2 — Single-Threaded Operations
//!
//! This module provides `get`, `put`, and `remove` without concurrency
//! control. Locking and OCC will be layered on in Phase 4.
//!
//! ## Node Type Detection
//!
//! Zig does not guarantee struct field ordering for regular structs.
//! We CANNOT cast an `*anyopaque` to `*NodeVersion` to call `is_leaf()`.
//! Instead, we track node types structurally:
//!
//! - `root_is_leaf: bool` in the tree struct.
//! - `internode.height == 0` means children are leaves.
//! - Layer roots always start as leaves.

const std = @import("std");
const Allocator = std.mem.Allocator;

const key_mod = @import("key.zig");
const Key = key_mod.Key;
const KSUF_KEYLENX = key_mod.KSUF_KEYLENX;
const LAYER_KEYLENX = key_mod.LAYER_KEYLENX;

const leaf_mod = @import("leaf.zig");
const interior_mod = @import("interior.zig");
const InternodeNode = interior_mod.InternodeNode;
const ver_mod = @import("node_version.zig");
const LockGuard = ver_mod.LockGuard;
const value_mod = @import("value.zig");
const perm_mod = @import("permuter.zig");
const Permuter15 = perm_mod.Permuter15;
const config = @import("config.zig");
const range_mod = @import("range.zig");

// ============================================================================
//  InsertSearchResult
// ============================================================================

/// Outcome of searching a leaf for an insert position.
const InsertSearchResult = union(enum) {
    /// Key exists at this physical slot.
    found: usize,
    /// Key not found; insert at this logical position.
    not_found: usize,
    /// Same ikey but different suffix — need to create a sublayer.
    conflict: usize,
    /// Slot holds a layer pointer — descend into sublayer.
    layer: usize,
};

// ============================================================================
//  MassTree(V)
// ============================================================================

/// A high-performance trie of B+ trees, generic over value type `V`.
pub fn MassTree(comptime V: type) type {
    const Leaf = leaf_mod.LeafNode(V);
    const LV = value_mod.LeafValue(V);

    return struct {
        const Self = @This();

        /// Tagged root pointer — low bit stores is_leaf flag.
        /// Use config.untag_ptr() to extract the pointer and type.
        root_tagged: usize,

        /// Number of key-value pairs in the tree.
        count: usize,

        /// Allocator used for all node allocations.
        allocator: Allocator,

        // ====================================================================
        //  Construction / Destruction
        // ====================================================================

        /// Create an empty MassTree.
        pub fn init(allocator: Allocator) Allocator.Error!Self {
            const leaf = try Leaf.init(allocator, true);
            return .{
                .root_tagged = config.tag_ptr(@ptrCast(leaf), true),
                .count = 0,
                .allocator = allocator,
            };
        }

        /// Destroy the tree, freeing all nodes recursively.
        pub fn deinit(self: *Self) void {
            const tagged = config.untag_ptr(self.root_tagged);
            self.destroy_node(tagged.ptr, tagged.is_leaf);
            self.root_tagged = 0;
            self.count = 0;
        }

        /// Return the number of key-value pairs.
        pub fn len(self: *const Self) usize {
            return @atomicLoad(usize, &self.count, .monotonic);
        }

        /// Check if the tree is empty.
        pub fn is_empty(self: *const Self) bool {
            return @atomicLoad(usize, &self.count, .monotonic) == 0;
        }

        // ====================================================================
        //  Range Iteration
        // ====================================================================

        /// Range bound specification (re-exported from range module).
        pub const RangeBound = range_mod.RangeBound;

        /// Forward range iterator type.
        pub const ForwardIterator = range_mod.RangeIterator(V);

        /// Reverse range iterator type.
        pub const ReverseIterator = range_mod.ReverseRangeIterator(V);

        /// Create a forward iterator over keys in [start, end].
        ///
        /// ```zig
        /// var it = tree.range(.{ .included = "aaa" }, .{ .excluded = "zzz" });
        /// while (it.next()) |entry| {
        ///     // entry.key, entry.value
        /// }
        /// ```
        pub fn range(self: *const Self, start: RangeBound, end: RangeBound) ForwardIterator {
            const root_tagged = @atomicLoad(usize, &self.root_tagged, .acquire);
            const tagged = config.untag_ptr(root_tagged);
            return ForwardIterator.init(tagged.ptr, tagged.is_leaf, start, end);
        }

        /// Create a reverse iterator over keys in [start, end] (descending order).
        pub fn range_reverse(self: *const Self, start: RangeBound, end: RangeBound) ReverseIterator {
            const root_tagged = @atomicLoad(usize, &self.root_tagged, .acquire);
            const tagged = config.untag_ptr(root_tagged);
            return ReverseIterator.init(tagged.ptr, tagged.is_leaf, start, end);
        }

        /// Create a forward iterator over all keys.
        pub fn range_all(self: *const Self) ForwardIterator {
            return self.range(.{ .unbounded = {} }, .{ .unbounded = {} });
        }

        // ====================================================================
        //  GET — with OCC (Optimistic Concurrency Control)
        // ====================================================================

        /// Look up a key and return its value, or null if not found.
        ///
        /// Uses the OCC protocol: at each node, take a version snapshot,
        /// perform reads, then verify the version hasn't changed.
        /// If any version check fails, restart from the tree root.
        pub fn get(self: *const Self, key_bytes: []const u8) ?V {
            // Full restart on any OCC validation failure
            retry: while (true) {
                var k = Key.init(key_bytes);
                // Atomically load the root pointer
                const root_tagged = @atomicLoad(usize, &self.root_tagged, .acquire);
                const root = config.untag_ptr(root_tagged);
                var node_ptr: *anyopaque = root.ptr;
                var is_leaf = root.is_leaf;

                // Layer loop: descend through trie layers
                while (true) {
                    // OCC navigate to the target leaf
                    var leaf = navigate_to_leaf_occ(node_ptr, is_leaf, &k) orelse
                        continue :retry;

                    // B-link forward: if key is beyond this leaf, walk
                    // the next-pointer chain to the correct leaf.
                    forward: while (true) {
                        // Take leaf version snapshot
                        const version = leaf.stable();

                        // Read under OCC
                        const result = search_leaf_for_get(leaf, &k);

                        // Validate: if leaf changed, restart this leaf
                        if (leaf.has_changed(version)) continue :forward;

                        // OCC passed — act on result
                        switch (result) {
                            .found => |slot| return leaf.get_value(slot),
                            .layer => |slot| {
                                const layer_info = leaf.get_layer(slot) orelse return null;
                                k.shift();
                                node_ptr = layer_info.ptr;
                                is_leaf = layer_info.is_leaf;
                                break :forward; // next layer
                            },
                            .not_found => {
                                // Check B-link: key might be in successor leaf
                                // due to concurrent split during internode navigation.
                                const next = leaf.load_next() orelse return null;
                                const next_perm = next.load_permutation();
                                if (next_perm.size() > 0) {
                                    const first_slot = next_perm.get(0);
                                    const boundary_ikey = next.ikeys[first_slot];
                                    if (k.ikey() >= boundary_ikey) {
                                        leaf = next;
                                        continue :forward;
                                    }
                                }
                                return null;
                            },
                        }
                    }
                }
            }
        }

        // ====================================================================
        //  PUT (insert or update) — with locking
        // ====================================================================

        /// Insert or update a key-value pair.
        /// Returns the old value if the key already existed.
        pub fn put(self: *Self, key_bytes: []const u8, val: V) Allocator.Error!?V {
            var k = Key.init(key_bytes);
            const root_tagged = @atomicLoad(usize, &self.root_tagged, .acquire);
            const root = config.untag_ptr(root_tagged);
            return self.put_at_layer(&k, val, root.ptr, root.is_leaf, true);
        }

        /// Internal recursive put with leaf locking.
        ///
        /// Navigate to the target leaf, lock it, search, then mutate
        /// while holding the lock. The lock is released before returning.
        ///
        /// Uses OCC navigation through internodes. After reaching a leaf,
        /// walks the B-link next-pointer chain with boundary checks to find
        /// the correct leaf, then locks it and performs the insert.
        fn put_at_layer(
            self: *Self,
            k: *Key,
            val: V,
            layer_root: *anyopaque,
            layer_root_is_leaf: bool,
            is_main_root: bool,
        ) Allocator.Error!?V {
            while (true) {
                // Reload root on each retry for main root: a concurrent split
                // may have created a new root above the stale layer_root.
                var current_root = layer_root;
                var current_is_leaf = layer_root_is_leaf;
                if (is_main_root) {
                    const rt = @atomicLoad(usize, &self.root_tagged, .acquire);
                    const rtp = config.untag_ptr(rt);
                    current_root = rtp.ptr;
                    current_is_leaf = rtp.is_leaf;
                }

                // OCC navigate to leaf (retries automatically on internode changes)
                const const_leaf = navigate_to_leaf_occ(current_root, current_is_leaf, k) orelse continue;
                var current: *Leaf = @constCast(const_leaf);

                // B-link forward: lock each candidate leaf, check boundary,
                // advance to successor if our key is beyond this leaf's range.
                while (true) {
                    var guard = current.lock();

                    // Check if key belongs in a successor leaf
                    if (current.load_next()) |next_leaf| {
                        const next_perm = next_leaf.load_permutation();
                        if (next_perm.size() > 0) {
                            const first_slot = next_perm.get(0);
                            const boundary_ikey = next_leaf.ikeys[first_slot];
                            if (k.ikey() >= boundary_ikey) {
                                guard.release();
                                current = next_leaf;
                                continue;
                            }
                        }
                    }

                    return self.put_at_leaf_locked(current, &guard, k, val, current_root, is_main_root);
                }
            }
        }

        /// Perform the insert on an already-locked leaf.
        fn put_at_leaf_locked(
            self: *Self,
            leaf: *Leaf,
            guard: *LockGuard,
            k: *Key,
            val: V,
            layer_root: *anyopaque,
            is_main_root: bool,
        ) Allocator.Error!?V {
            const search = search_leaf_for_insert(leaf, k);

            switch (search) {
                .found => |slot| {
                    const old = leaf.get_value(slot);
                    leaf.set_value(slot, val);
                    guard.release();
                    return old;
                },
                .not_found => |logical_pos| {
                    if (!leaf.is_full()) {
                        guard.mark_insert();
                        _ = try leaf.insert_key(logical_pos, k.*, LV.init_value(val));
                        guard.release();
                        _ = @atomicRmw(usize, &self.count, .Add, 1, .monotonic);
                        return null;
                    }
                    guard.mark_insert();
                    const r = self.handle_split_and_insert(
                        leaf,
                        guard,
                        logical_pos,
                        k,
                        val,
                        layer_root,
                        is_main_root,
                    );
                    return r;
                },
                .conflict => |slot| {
                    guard.mark_insert();
                    try self.create_layer(leaf, slot, k, val);
                    guard.release();
                    _ = @atomicRmw(usize, &self.count, .Add, 1, .monotonic);
                    return null;
                },
                .layer => |slot| {
                    const layer_info = leaf.get_layer(slot) orelse {
                        guard.release();
                        return error.OutOfMemory;
                    };
                    guard.release();
                    k.shift();
                    return self.put_at_layer(k, val, layer_info.ptr, layer_info.is_leaf, false);
                },
            }
        }

        // ====================================================================
        //  REMOVE — with locking
        // ====================================================================

        /// Remove a key from the tree.
        /// Returns the removed value, or null if the key didn't exist.
        pub fn remove(self: *Self, key_bytes: []const u8) ?V {
            var k = Key.init(key_bytes);
            const root_tagged = @atomicLoad(usize, &self.root_tagged, .acquire);
            const root = config.untag_ptr(root_tagged);
            return self.remove_at_layer(&k, root.ptr, root.is_leaf, true);
        }

        /// Internal recursive remove with leaf locking.
        /// Uses OCC navigation then B-link boundary checks to find correct leaf.
        fn remove_at_layer(self: *Self, k: *Key, layer_root: *anyopaque, layer_is_leaf: bool, is_main_root: bool) ?V {
            while (true) {
                // Reload root on each retry for main root.
                var current_root = layer_root;
                var current_is_leaf = layer_is_leaf;
                if (is_main_root) {
                    const rt = @atomicLoad(usize, &self.root_tagged, .acquire);
                    const rtp = config.untag_ptr(rt);
                    current_root = rtp.ptr;
                    current_is_leaf = rtp.is_leaf;
                }

                // OCC navigate to leaf (retries automatically on internode changes)
                const const_leaf = navigate_to_leaf_occ(current_root, current_is_leaf, k) orelse continue;
                var current: *Leaf = @constCast(const_leaf);

                // B-link forward: lock, check boundary, advance if needed.
                while (true) {
                    var guard = current.lock();

                    if (current.load_next()) |next_leaf| {
                        const next_perm = next_leaf.load_permutation();
                        if (next_perm.size() > 0) {
                            const first_slot = next_perm.get(0);
                            const boundary_ikey = next_leaf.ikeys[first_slot];
                            if (k.ikey() >= boundary_ikey) {
                                guard.release();
                                current = next_leaf;
                                continue;
                            }
                        }
                    }

                    return self.remove_from_locked_leaf(current, &guard, k);
                }
            }
        }

        /// Perform remove on an already-locked leaf.
        fn remove_from_locked_leaf(self: *Self, leaf: *Leaf, guard: *LockGuard, k: *Key) ?V {
            const result = search_leaf_for_get(
                @as(*const Leaf, @ptrCast(@alignCast(leaf))),
                k,
            );

            switch (result) {
                .found => |slot| {
                    const old_val = leaf.get_value(slot);
                    leaf.remove_slot_entry(slot);
                    guard.release();
                    _ = @atomicRmw(usize, &self.count, .Sub, 1, .monotonic);
                    return old_val;
                },
                .layer => |slot| {
                    const layer_info = leaf.get_layer(slot) orelse {
                        guard.release();
                        return null;
                    };
                    guard.release();
                    k.shift();
                    return self.remove_at_layer(k, layer_info.ptr, layer_info.is_leaf, false);
                },
                .not_found => {
                    guard.release();
                    return null;
                },
            }
        }

        // ====================================================================
        //  Tree Traversal
        // ====================================================================

        /// Navigate to a leaf using OCC on each internode.
        ///
        /// At each internode: take version snapshot, read routing keys,
        /// select child, then verify version. If any internode changed,
        /// returns null to signal the caller to retry from the layer root.
        fn navigate_to_leaf_occ(root: *anyopaque, root_is_leaf: bool, k: *const Key) ?*const Leaf {
            if (root_is_leaf) {
                return @ptrCast(@alignCast(root));
            }

            var inode: *const InternodeNode = @ptrCast(@alignCast(root));
            while (true) {
                const ver = inode.stable();

                // Use atomic nkeys load to avoid reading a partially-updated
                // count while the internode is being modified under lock
                // (without INSERTING_BIT set).
                const n = inode.load_nkeys();
                var child_idx: usize = 0;
                const search_ikey = k.ikey();
                while (child_idx < n) : (child_idx += 1) {
                    if (search_ikey < inode.ikeys[child_idx]) break;
                    if (search_ikey == inode.ikeys[child_idx]) {
                        child_idx += 1;
                        break;
                    }
                }
                const child = inode.load_child(child_idx) orelse return null;

                // Validate internode read
                if (inode.has_changed(ver)) return null;

                if (inode.height == 0) {
                    // Child is a leaf
                    return @ptrCast(@alignCast(child));
                }

                // Child is an internode — descend
                inode = @ptrCast(@alignCast(child));
            }
        }

        // ====================================================================
        //  Leaf Search
        // ====================================================================

        /// Result for get search.
        const GetSearchResult = union(enum) {
            found: usize,
            layer: usize,
            not_found: void,
        };

        /// Search a leaf for a key (get/remove path).
        fn search_leaf_for_get(leaf: *const Leaf, k: *const Key) GetSearchResult {
            const target_ikey = k.ikey();
            const perm = leaf.permutation;
            const s = perm.size();

            const search_keylenx: u8 = if (k.has_suffix())
                KSUF_KEYLENX
            else
                @intCast(k.current_len());

            var i: usize = 0;
            while (i < s) : (i += 1) {
                const slot = perm.get(i);
                const slot_ikey = leaf.ikeys[slot];

                if (slot_ikey == target_ikey) {
                    const slot_klx = leaf.keylenx[slot];

                    if (slot_klx >= LAYER_KEYLENX) {
                        if (k.has_suffix()) return .{ .layer = slot };
                        return .not_found;
                    }

                    if (slot_klx == search_keylenx) {
                        if (slot_klx == KSUF_KEYLENX) {
                            if (leaf.ksuf_equals(slot, k.suffix())) {
                                return .{ .found = slot };
                            }
                            continue;
                        }
                        return .{ .found = slot };
                    }
                    continue;
                } else if (slot_ikey > target_ikey) {
                    return .not_found;
                }
            }
            return .not_found;
        }

        /// Search a leaf for insert position.
        fn search_leaf_for_insert(leaf: *const Leaf, k: *const Key) InsertSearchResult {
            const target_ikey = k.ikey();
            const perm = leaf.permutation;
            const s = perm.size();

            const search_keylenx: u8 = if (k.has_suffix())
                KSUF_KEYLENX
            else
                @intCast(k.current_len());

            var i: usize = 0;
            while (i < s) : (i += 1) {
                const slot = perm.get(i);
                const slot_ikey = leaf.ikeys[slot];

                if (slot_ikey == target_ikey) {
                    const slot_klx = leaf.keylenx[slot];

                    if (leaf.values[slot].is_empty()) continue;

                    if (slot_klx >= LAYER_KEYLENX) {
                        if (k.has_suffix()) return .{ .layer = slot };
                        return .{ .not_found = i };
                    }

                    if (slot_klx == search_keylenx) {
                        if (slot_klx == KSUF_KEYLENX) {
                            if (leaf.ksuf_equals(slot, k.suffix())) {
                                return .{ .found = slot };
                            }
                            return .{ .conflict = slot };
                        }
                        return .{ .found = slot };
                    }

                    if (slot_klx == KSUF_KEYLENX and search_keylenx == KSUF_KEYLENX) {
                        return .{ .conflict = slot };
                    }

                    if (search_keylenx < slot_klx) {
                        return .{ .not_found = i };
                    }
                    continue;
                } else if (slot_ikey > target_ikey) {
                    return .{ .not_found = i };
                }
            }
            return .{ .not_found = s };
        }

        // ====================================================================
        //  Split Handling — with locking
        // ====================================================================

        /// Handle a leaf split and insert.
        /// Caller holds the leaf lock (guard). This function releases it.
        fn handle_split_and_insert(
            self: *Self,
            leaf: *Leaf,
            guard: *LockGuard,
            logical_pos: usize,
            k: *const Key,
            val: V,
            layer_root: *anyopaque,
            is_main_root: bool,
        ) Allocator.Error!?V {
            const ik = k.ikey();
            const klx: u8 = if (k.has_suffix()) KSUF_KEYLENX else @intCast(k.current_len());
            const suf: ?[]const u8 = if (k.has_suffix()) k.suffix() else null;

            const sp = leaf.calculate_split_point(logical_pos, ik) orelse {
                guard.release();
                return error.OutOfMemory;
            };

            const right = try Leaf.init(self.allocator, false);
            errdefer right.deinit(self.allocator);

            const result = try leaf.split_and_insert(
                sp.pos,
                right,
                logical_pos,
                ik,
                klx,
                LV.init_value(val),
                suf,
            );
            _ = result;

            // Mark split on the guard (increments vsplit counter)
            guard.mark_split();

            // Note: split_into already links next/prev pointers.

            const sep_slot = right.permutation.get(0);
            const sep_ikey = right.ikeys[sep_slot];

            try self.propagate_split(
                @ptrCast(leaf),
                @ptrCast(right),
                sep_ikey,
                layer_root,
                is_main_root,
                true,
            );

            // propagate_split may have cleared ROOT_BIT on the leaf via
            // create_new_root → mark_node_nonroot (uses @atomicRmw).
            // Sync the guard's locked_value so release() doesn't clobber it.
            const actual_root_bit = @atomicLoad(u32, &leaf.version.value, .monotonic) & ver_mod.NodeVersion.ROOT_BIT;
            guard.locked_value = (guard.locked_value & ~ver_mod.NodeVersion.ROOT_BIT) | actual_root_bit;

            // Release the leaf lock
            guard.release();
            _ = @atomicRmw(usize, &self.count, .Add, 1, .monotonic);
            return null;
        }

        /// Propagate a split upward through internodes.
        /// Uses hand-over-hand locking: locks parent before modifying.
        fn propagate_split(
            self: *Self,
            left_ptr: *anyopaque,
            right_ptr: *anyopaque,
            split_ikey: u64,
            layer_root: *anyopaque,
            is_main_root: bool,
            at_leaf_level: bool,
        ) Allocator.Error!void {
            var left = left_ptr;
            var right = right_ptr;
            var s_ikey = split_ikey;
            var ly_root = layer_root;
            var main_root = is_main_root;
            var leaf_level = at_leaf_level;

            while (true) {
                const parent_ptr: ?*anyopaque = get_node_parent(left, leaf_level);

                if (parent_ptr == null) {
                    try self.create_new_root(left, right, s_ikey, ly_root, main_root, leaf_level);
                    return;
                }

                const parent: *InternodeNode = @ptrCast(@alignCast(parent_ptr.?));
                var parent_guard = parent.lock();

                if (!parent.is_full()) {
                    const child_idx = parent.find_child_index(left) orelse 0;
                    parent.insert_key_and_child(child_idx, s_ikey, right);
                    set_node_parent(right, @ptrCast(parent), leaf_level);
                    parent_guard.release();
                    return;
                }

                // Parent full — split it
                parent_guard.mark_insert();
                parent_guard.mark_split();

                const new_sibling = try InternodeNode.init_for_split(self.allocator, parent.height);
                const child_idx = parent.find_child_index(left) orelse 0;
                const split_result = parent.split_into(
                    new_sibling,
                    @ptrCast(new_sibling),
                    child_idx,
                    s_ikey,
                    right,
                );

                if (split_result.insert_went_left) {
                    set_node_parent(right, @ptrCast(parent), leaf_level);
                } else {
                    set_node_parent(right, @ptrCast(new_sibling), leaf_level);
                }

                reparent_internode_children(new_sibling);

                // Release parent lock before continuing upward
                parent_guard.release();

                left = @ptrCast(parent);
                right = @ptrCast(new_sibling);
                s_ikey = split_result.popup_key;
                ly_root = ly_root;
                main_root = (left == config.untag_ptr(self.root_tagged).ptr);
                leaf_level = false;
            }
        }

        /// Create a new root internode above two children.
        fn create_new_root(
            self: *Self,
            left: *anyopaque,
            right: *anyopaque,
            split_ikey: u64,
            layer_root: *anyopaque,
            is_main_root: bool,
            at_leaf_level: bool,
        ) Allocator.Error!void {
            const height: u8 = if (at_leaf_level) 0 else blk: {
                const child: *const InternodeNode = @ptrCast(@alignCast(left));
                break :blk child.height + 1;
            };

            const new_root = try InternodeNode.init_root(self.allocator, height);
            new_root.ikeys[0] = split_ikey;
            new_root.children[0] = left;
            new_root.children[1] = right;
            new_root.nkeys = 1;

            set_node_parent(left, @ptrCast(new_root), at_leaf_level);
            set_node_parent(right, @ptrCast(new_root), at_leaf_level);
            mark_node_nonroot(left, at_leaf_level);

            if (is_main_root) {
                // Atomically publish the new root
                @atomicStore(usize, &self.root_tagged, config.tag_ptr(@ptrCast(new_root), false), .release);
            } else {
                self.update_layer_root_pointer(layer_root, @ptrCast(new_root));
            }
        }

        /// When a sublayer root splits and creates a new root, find the
        /// parent leaf slot that points to the old layer_root and update it.
        fn update_layer_root_pointer(self: *Self, old_root: *anyopaque, new_root: *anyopaque) void {
            const root = config.untag_ptr(self.root_tagged);
            self.find_and_replace_layer_ptr(root.ptr, root.is_leaf, old_root, new_root);
        }

        /// Recursively search for a layer pointer and replace it.
        fn find_and_replace_layer_ptr(
            self: *Self,
            node: *anyopaque,
            is_leaf: bool,
            old_ptr: *anyopaque,
            new_ptr: *anyopaque,
        ) void {
            if (is_leaf) {
                const leaf: *Leaf = @ptrCast(@alignCast(node));
                const perm = leaf.permutation;
                const s = perm.size();

                for (0..s) |i| {
                    const slot = perm.get(i);
                    if (leaf.keylenx[slot] >= LAYER_KEYLENX) {
                        if (leaf.values[slot].try_as_layer()) |layer_info| {
                            if (layer_info.ptr == old_ptr) {
                                // Replacing a sublayer root with a new internode root
                                leaf.values[slot] = LV.init_layer(new_ptr, false);
                                return;
                            }
                            // Recurse into sublayer
                            self.find_and_replace_layer_ptr(layer_info.ptr, layer_info.is_leaf, old_ptr, new_ptr);
                        }
                    }
                }
            } else {
                const inode: *const InternodeNode = @ptrCast(@alignCast(node));
                const n: usize = @as(usize, inode.nkeys) + 1;
                const children_are_leaves = (inode.height == 0);
                for (0..n) |i| {
                    if (inode.children[i]) |child| {
                        self.find_and_replace_layer_ptr(child, children_are_leaves, old_ptr, new_ptr);
                    }
                }
            }
        }

        // ====================================================================
        //  Layer Creation
        // ====================================================================

        /// Create a new sublayer when two keys share the same 8-byte ikey
        /// but have different suffixes.
        fn create_layer(
            self: *Self,
            parent_leaf: *Leaf,
            conflict_slot: usize,
            new_key: *Key,
            new_value: V,
        ) Allocator.Error!void {
            const existing_suf = parent_leaf.ksuf(conflict_slot) orelse &[_]u8{};
            const existing_val = parent_leaf.get_value(conflict_slot);

            var existing_key = Key.from_suffix(existing_suf, 0);
            new_key.shift();

            // Build twig chain while both keys share the same ikey
            var layer_root_ptr: ?*anyopaque = null;
            var prev_twig: ?*Leaf = null;
            var prev_twig_slot: usize = 0;

            while (existing_key.ikey() == new_key.ikey() and
                existing_key.has_suffix() and new_key.has_suffix())
            {
                const twig = try Leaf.init_layer_root(self.allocator);
                twig.ikeys[0] = existing_key.ikey();
                twig.keylenx[0] = LAYER_KEYLENX;
                twig.permutation = Permuter15.make_sorted(1);

                if (prev_twig) |pt| {
                    pt.values[prev_twig_slot] = LV.init_layer(@ptrCast(twig), true);
                } else {
                    layer_root_ptr = @ptrCast(twig);
                }

                prev_twig = twig;
                prev_twig_slot = 0;
                existing_key.shift();
                new_key.shift();
            }

            // Create final leaf with both entries
            const final_leaf = try Leaf.init_layer_root(self.allocator);
            const ex_ik = existing_key.ikey();
            const nw_ik = new_key.ikey();

            if (ex_ik < nw_ik) {
                try assign_entry_from_key(final_leaf, 0, &existing_key, existing_val);
                try assign_entry_from_key_val(final_leaf, 1, new_key, new_value);
            } else if (ex_ik > nw_ik) {
                try assign_entry_from_key_val(final_leaf, 0, new_key, new_value);
                try assign_entry_from_key(final_leaf, 1, &existing_key, existing_val);
            } else {
                if (existing_key.current_len() <= new_key.current_len()) {
                    try assign_entry_from_key(final_leaf, 0, &existing_key, existing_val);
                    try assign_entry_from_key_val(final_leaf, 1, new_key, new_value);
                } else {
                    try assign_entry_from_key_val(final_leaf, 0, new_key, new_value);
                    try assign_entry_from_key(final_leaf, 1, &existing_key, existing_val);
                }
            }

            final_leaf.permutation = Permuter15.make_sorted(2);

            // Link the chain
            if (prev_twig) |pt| {
                pt.values[prev_twig_slot] = LV.init_layer(@ptrCast(final_leaf), true);
            } else {
                layer_root_ptr = @ptrCast(final_leaf);
            }

            parent_leaf.make_layer(conflict_slot, layer_root_ptr.?, true);
        }

        /// Assign a key-value pair to a physical slot from a Key + optional value.
        fn assign_entry_from_key(leaf: *Leaf, slot: usize, k: *const Key, val: ?V) Allocator.Error!void {
            const ik = k.ikey();
            const klx: u8 = if (k.has_suffix()) KSUF_KEYLENX else @intCast(k.current_len());
            const suf: ?[]const u8 = if (k.has_suffix()) k.suffix() else null;
            const lv: LV = if (val) |v| LV.init_value(v) else LV.init_empty();
            try leaf.assign_slot(slot, ik, klx, lv, suf);
        }

        /// Assign a key-value pair to a physical slot from a Key + value.
        fn assign_entry_from_key_val(leaf: *Leaf, slot: usize, k: *const Key, val: V) Allocator.Error!void {
            const ik = k.ikey();
            const klx: u8 = if (k.has_suffix()) KSUF_KEYLENX else @intCast(k.current_len());
            const suf: ?[]const u8 = if (k.has_suffix()) k.suffix() else null;
            try leaf.assign_slot(slot, ik, klx, LV.init_value(val), suf);
        }

        // ====================================================================
        //  Node Helpers
        // ====================================================================

        fn get_node_parent(node: *anyopaque, at_leaf_level: bool) ?*anyopaque {
            if (at_leaf_level) {
                const leaf: *const Leaf = @ptrCast(@alignCast(node));
                return leaf.parent;
            } else {
                const inode: *const InternodeNode = @ptrCast(@alignCast(node));
                return inode.get_parent();
            }
        }

        fn set_node_parent(node: *anyopaque, parent: *anyopaque, at_leaf_level: bool) void {
            if (at_leaf_level) {
                const leaf: *Leaf = @ptrCast(@alignCast(node));
                leaf.parent = parent;
            } else {
                const inode: *InternodeNode = @ptrCast(@alignCast(node));
                inode.set_parent(parent);
            }
        }

        fn mark_node_nonroot(node: *anyopaque, at_leaf_level: bool) void {
            if (at_leaf_level) {
                const leaf: *Leaf = @ptrCast(@alignCast(node));
                leaf.version.mark_nonroot();
            } else {
                const inode: *InternodeNode = @ptrCast(@alignCast(node));
                inode.version.mark_nonroot();
            }
        }

        fn reparent_internode_children(new_sibling: *InternodeNode) void {
            const n: usize = @as(usize, new_sibling.nkeys) + 1;
            if (new_sibling.height == 0) {
                for (0..n) |i| {
                    if (new_sibling.children[i]) |child_ptr| {
                        const child: *Leaf = @ptrCast(@alignCast(child_ptr));
                        child.parent = @ptrCast(new_sibling);
                    }
                }
            } else {
                for (0..n) |i| {
                    if (new_sibling.children[i]) |child_ptr| {
                        const child: *InternodeNode = @ptrCast(@alignCast(child_ptr));
                        child.set_parent(@ptrCast(new_sibling));
                    }
                }
            }
        }

        // ====================================================================
        //  Recursive Destruction
        // ====================================================================

        /// Recursively free all nodes in the tree.
        fn destroy_node(self: *Self, node: *anyopaque, is_leaf: bool) void {
            if (is_leaf) {
                const leaf: *Leaf = @ptrCast(@alignCast(node));
                // Recursively destroy sublayers
                const perm = leaf.permutation;
                const s = perm.size();
                for (0..s) |i| {
                    const slot = perm.get(i);
                    if (leaf.keylenx[slot] >= LAYER_KEYLENX) {
                        if (leaf.values[slot].try_as_layer()) |layer_info| {
                            self.destroy_node(layer_info.ptr, layer_info.is_leaf);
                        }
                    }
                }
                leaf.deinit(self.allocator);
            } else {
                const inode: *InternodeNode = @ptrCast(@alignCast(node));
                const n: usize = @as(usize, inode.nkeys) + 1;
                const children_are_leaves = (inode.height == 0);
                for (0..n) |i| {
                    if (inode.children[i]) |child| {
                        self.destroy_node(child, children_are_leaves);
                    }
                }
                inode.deinit(self.allocator);
            }
        }
    };
}

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "MassTree: init and deinit" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    try testing.expectEqual(@as(usize, 0), tree.len());
    try testing.expect(tree.is_empty());
}

test "MassTree: put and get single key" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const old = try tree.put("hello", 42);
    try testing.expect(old == null);
    try testing.expectEqual(@as(usize, 1), tree.len());

    const val = tree.get("hello");
    try testing.expectEqual(@as(u64, 42), val.?);
}

test "MassTree: put updates existing key" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("key", 1);
    const old = try tree.put("key", 2);
    try testing.expectEqual(@as(u64, 1), old.?);

    const val = tree.get("key");
    try testing.expectEqual(@as(u64, 2), val.?);
    try testing.expectEqual(@as(usize, 1), tree.len());
}

test "MassTree: get non-existent key" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("exists", 1);
    try testing.expect(tree.get("nope") == null);
}

test "MassTree: remove key" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("key", 42);
    const removed = tree.remove("key");
    try testing.expectEqual(@as(u64, 42), removed.?);
    try testing.expectEqual(@as(usize, 0), tree.len());
    try testing.expect(tree.get("key") == null);
}

test "MassTree: remove non-existent key" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    try testing.expect(tree.remove("nope") == null);
}

test "MassTree: multiple keys no split" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaa", 1);
    _ = try tree.put("bbb", 2);
    _ = try tree.put("ccc", 3);

    try testing.expectEqual(@as(usize, 3), tree.len());
    try testing.expectEqual(@as(u64, 1), tree.get("aaa").?);
    try testing.expectEqual(@as(u64, 2), tree.get("bbb").?);
    try testing.expectEqual(@as(u64, 3), tree.get("ccc").?);
}

test "MassTree: leaf split 16 keys" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    var i: u64 = 0;
    while (i < 16) : (i += 1) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        _ = try tree.put(&buf, i);
    }

    try testing.expectEqual(@as(usize, 16), tree.len());

    i = 0;
    while (i < 16) : (i += 1) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        const val = tree.get(&buf);
        try testing.expectEqual(i, val.?);
    }
}

test "MassTree: many keys multiple splits" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const N: u64 = 100;
    var i: u64 = 0;
    while (i < N) : (i += 1) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        _ = try tree.put(&buf, i * 10);
    }

    try testing.expectEqual(@as(usize, @intCast(N)), tree.len());

    i = 0;
    while (i < N) : (i += 1) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        const val = tree.get(&buf);
        try testing.expectEqual(i * 10, val.?);
    }
}

test "MassTree: long keys layer creation" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaaaaaaa_suffix1", 1);
    _ = try tree.put("aaaaaaaa_suffix2", 2);

    try testing.expectEqual(@as(usize, 2), tree.len());
    try testing.expectEqual(@as(u64, 1), tree.get("aaaaaaaa_suffix1").?);
    try testing.expectEqual(@as(u64, 2), tree.get("aaaaaaaa_suffix2").?);
}

test "MassTree: long keys different prefixes" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("prefix01_val", 1);
    _ = try tree.put("prefix02_val", 2);
    _ = try tree.put("prefix03_val", 3);

    try testing.expectEqual(@as(usize, 3), tree.len());
    try testing.expectEqual(@as(u64, 1), tree.get("prefix01_val").?);
    try testing.expectEqual(@as(u64, 2), tree.get("prefix02_val").?);
    try testing.expectEqual(@as(u64, 3), tree.get("prefix03_val").?);
}

test "MassTree: remove with layer" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaaaaaaa_one", 10);
    _ = try tree.put("aaaaaaaa_two", 20);

    const removed = tree.remove("aaaaaaaa_one");
    try testing.expectEqual(@as(u64, 10), removed.?);
    try testing.expectEqual(@as(usize, 1), tree.len());

    try testing.expectEqual(@as(u64, 20), tree.get("aaaaaaaa_two").?);
    try testing.expect(tree.get("aaaaaaaa_one") == null);
}

test "MassTree: stress mixed operations" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const N: u64 = 200;
    var i: u64 = 0;
    while (i < N) : (i += 1) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        _ = try tree.put(&buf, i);
    }
    try testing.expectEqual(@as(usize, @intCast(N)), tree.len());

    // Remove even keys
    i = 0;
    while (i < N) : (i += 2) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        const r = tree.remove(&buf);
        try testing.expect(r != null);
    }
    try testing.expectEqual(@as(usize, @intCast(N / 2)), tree.len());

    // Verify odd keys still exist
    i = 1;
    while (i < N) : (i += 2) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        const val = tree.get(&buf);
        try testing.expectEqual(i, val.?);
    }

    // Verify even keys are gone
    i = 0;
    while (i < N) : (i += 2) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        try testing.expect(tree.get(&buf) == null);
    }
}

test "MassTree: empty key" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("", 999);
    try testing.expectEqual(@as(u64, 999), tree.get("").?);
    try testing.expectEqual(@as(usize, 1), tree.len());
}

test "MassTree: keys various lengths" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("a", 1);
    _ = try tree.put("ab", 2);
    _ = try tree.put("abc", 3);
    _ = try tree.put("abcdefgh", 8);
    _ = try tree.put("abcdefghi", 9);
    _ = try tree.put("abcdefghijklmnop", 16);

    try testing.expectEqual(@as(usize, 6), tree.len());
    try testing.expectEqual(@as(u64, 1), tree.get("a").?);
    try testing.expectEqual(@as(u64, 2), tree.get("ab").?);
    try testing.expectEqual(@as(u64, 3), tree.get("abc").?);
    try testing.expectEqual(@as(u64, 8), tree.get("abcdefgh").?);
    try testing.expectEqual(@as(u64, 9), tree.get("abcdefghi").?);
    try testing.expectEqual(@as(u64, 16), tree.get("abcdefghijklmnop").?);
}

test "MassTree: large stress 1000 keys" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const N: u64 = 1000;
    var i: u64 = 0;
    while (i < N) : (i += 1) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        _ = try tree.put(&buf, i);
    }
    try testing.expectEqual(@as(usize, @intCast(N)), tree.len());

    // Verify all
    i = 0;
    while (i < N) : (i += 1) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        try testing.expectEqual(i, tree.get(&buf).?);
    }

    // Remove all
    i = 0;
    while (i < N) : (i += 1) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        try testing.expectEqual(i, tree.remove(&buf).?);
    }
    try testing.expectEqual(@as(usize, 0), tree.len());
}

// ============================================================================
//  Range Iterator Tests
// ============================================================================

test "MassTree: range_all forward" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("bbb", 2);
    _ = try tree.put("aaa", 1);
    _ = try tree.put("ccc", 3);

    var it = tree.range_all();
    const e1 = it.next().?;
    try testing.expectEqualSlices(u8, "aaa", e1.key);
    try testing.expectEqual(@as(u64, 1), e1.value);

    const e2 = it.next().?;
    try testing.expectEqualSlices(u8, "bbb", e2.key);
    try testing.expectEqual(@as(u64, 2), e2.value);

    const e3 = it.next().?;
    try testing.expectEqualSlices(u8, "ccc", e3.key);
    try testing.expectEqual(@as(u64, 3), e3.value);

    try testing.expect(it.next() == null);
}

test "MassTree: range with included bounds" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaa", 1);
    _ = try tree.put("bbb", 2);
    _ = try tree.put("ccc", 3);
    _ = try tree.put("ddd", 4);
    _ = try tree.put("eee", 5);

    var it = tree.range(.{ .included = "bbb" }, .{ .included = "ddd" });
    const e1 = it.next().?;
    try testing.expectEqualSlices(u8, "bbb", e1.key);
    try testing.expectEqual(@as(u64, 2), e1.value);

    const e2 = it.next().?;
    try testing.expectEqualSlices(u8, "ccc", e2.key);
    try testing.expectEqual(@as(u64, 3), e2.value);

    const e3 = it.next().?;
    try testing.expectEqualSlices(u8, "ddd", e3.key);
    try testing.expectEqual(@as(u64, 4), e3.value);

    try testing.expect(it.next() == null);
}

test "MassTree: range with excluded bounds" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaa", 1);
    _ = try tree.put("bbb", 2);
    _ = try tree.put("ccc", 3);
    _ = try tree.put("ddd", 4);
    _ = try tree.put("eee", 5);

    var it = tree.range(.{ .excluded = "bbb" }, .{ .excluded = "eee" });
    const e1 = it.next().?;
    try testing.expectEqualSlices(u8, "ccc", e1.key);
    try testing.expectEqual(@as(u64, 3), e1.value);

    const e2 = it.next().?;
    try testing.expectEqualSlices(u8, "ddd", e2.key);
    try testing.expectEqual(@as(u64, 4), e2.value);

    try testing.expect(it.next() == null);
}

test "MassTree: range with unbounded start" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaa", 1);
    _ = try tree.put("bbb", 2);
    _ = try tree.put("ccc", 3);

    var it = tree.range(.{ .unbounded = {} }, .{ .included = "bbb" });
    const e1 = it.next().?;
    try testing.expectEqualSlices(u8, "aaa", e1.key);

    const e2 = it.next().?;
    try testing.expectEqualSlices(u8, "bbb", e2.key);

    try testing.expect(it.next() == null);
}

test "MassTree: range with unbounded end" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaa", 1);
    _ = try tree.put("bbb", 2);
    _ = try tree.put("ccc", 3);

    var it = tree.range(.{ .included = "bbb" }, .{ .unbounded = {} });
    const e1 = it.next().?;
    try testing.expectEqualSlices(u8, "bbb", e1.key);

    const e2 = it.next().?;
    try testing.expectEqualSlices(u8, "ccc", e2.key);

    try testing.expect(it.next() == null);
}

test "MassTree: range empty result" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaa", 1);
    _ = try tree.put("zzz", 2);

    var it = tree.range(.{ .included = "mmm" }, .{ .included = "nnn" });
    try testing.expect(it.next() == null);
}

test "MassTree: range empty tree" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    var it = tree.range_all();
    try testing.expect(it.next() == null);
}

test "MassTree: range with many keys across splits" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const N: u64 = 100;
    var i: u64 = 0;
    while (i < N) : (i += 1) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        _ = try tree.put(&buf, i);
    }

    // Iterate all and check ascending order
    var it = tree.range_all();
    var count: usize = 0;
    var last_key: ?u64 = null;
    while (it.next()) |entry| {
        const k = std.mem.readInt(u64, entry.key[0..8], .big);
        if (last_key) |lk| {
            try testing.expect(k > lk);
        }
        last_key = k;
        count += 1;
    }
    try testing.expectEqual(@as(usize, @intCast(N)), count);
}

test "MassTree: range bounded subset of many keys" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const N: u64 = 100;
    var i: u64 = 0;
    while (i < N) : (i += 1) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        _ = try tree.put(&buf, i * 10);
    }

    // Range [20, 30) — should get keys 20..29
    var start_buf: [8]u8 = undefined;
    var end_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &start_buf, 20, .big);
    std.mem.writeInt(u64, &end_buf, 30, .big);

    var it = tree.range(.{ .included = &start_buf }, .{ .excluded = &end_buf });
    var count: usize = 0;
    var expected: u64 = 20;
    while (it.next()) |entry| {
        const k = std.mem.readInt(u64, entry.key[0..8], .big);
        try testing.expectEqual(expected, k);
        try testing.expectEqual(expected * 10, entry.value);
        expected += 1;
        count += 1;
    }
    try testing.expectEqual(@as(usize, 10), count);
}

test "MassTree: range with long keys (layers)" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaaaaaaa_alpha", 1);
    _ = try tree.put("aaaaaaaa_beta", 2);
    _ = try tree.put("aaaaaaaa_gamma", 3);
    _ = try tree.put("bbbb", 4);

    // All keys
    var it = tree.range_all();
    const e1 = it.next().?;
    try testing.expectEqualSlices(u8, "aaaaaaaa_alpha", e1.key);
    try testing.expectEqual(@as(u64, 1), e1.value);

    const e2 = it.next().?;
    try testing.expectEqualSlices(u8, "aaaaaaaa_beta", e2.key);
    try testing.expectEqual(@as(u64, 2), e2.value);

    const e3 = it.next().?;
    try testing.expectEqualSlices(u8, "aaaaaaaa_gamma", e3.key);
    try testing.expectEqual(@as(u64, 3), e3.value);

    const e4 = it.next().?;
    try testing.expectEqualSlices(u8, "bbbb", e4.key);
    try testing.expectEqual(@as(u64, 4), e4.value);

    try testing.expect(it.next() == null);
}

test "MassTree: range bounded within layer" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaaaaaaa_alpha", 1);
    _ = try tree.put("aaaaaaaa_beta", 2);
    _ = try tree.put("aaaaaaaa_gamma", 3);

    var it = tree.range(
        .{ .included = "aaaaaaaa_beta" },
        .{ .included = "aaaaaaaa_gamma" },
    );
    const e1 = it.next().?;
    try testing.expectEqualSlices(u8, "aaaaaaaa_beta", e1.key);

    const e2 = it.next().?;
    try testing.expectEqualSlices(u8, "aaaaaaaa_gamma", e2.key);

    try testing.expect(it.next() == null);
}

test "MassTree: reverse range_all" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaa", 1);
    _ = try tree.put("bbb", 2);
    _ = try tree.put("ccc", 3);

    var it = tree.range_reverse(.{ .unbounded = {} }, .{ .unbounded = {} });
    const e1 = it.next().?;
    try testing.expectEqualSlices(u8, "ccc", e1.key);
    try testing.expectEqual(@as(u64, 3), e1.value);

    const e2 = it.next().?;
    try testing.expectEqualSlices(u8, "bbb", e2.key);
    try testing.expectEqual(@as(u64, 2), e2.value);

    const e3 = it.next().?;
    try testing.expectEqualSlices(u8, "aaa", e3.key);
    try testing.expectEqual(@as(u64, 1), e3.value);

    try testing.expect(it.next() == null);
}

test "MassTree: reverse range with bounds" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaa", 1);
    _ = try tree.put("bbb", 2);
    _ = try tree.put("ccc", 3);
    _ = try tree.put("ddd", 4);
    _ = try tree.put("eee", 5);

    var it = tree.range_reverse(.{ .included = "bbb" }, .{ .included = "ddd" });
    const e1 = it.next().?;
    try testing.expectEqualSlices(u8, "ddd", e1.key);
    try testing.expectEqual(@as(u64, 4), e1.value);

    const e2 = it.next().?;
    try testing.expectEqualSlices(u8, "ccc", e2.key);
    try testing.expectEqual(@as(u64, 3), e2.value);

    const e3 = it.next().?;
    try testing.expectEqualSlices(u8, "bbb", e3.key);
    try testing.expectEqual(@as(u64, 2), e3.value);

    try testing.expect(it.next() == null);
}

test "MassTree: reverse range with layers" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaaaaaaa_alpha", 1);
    _ = try tree.put("aaaaaaaa_beta", 2);
    _ = try tree.put("aaaaaaaa_gamma", 3);
    _ = try tree.put("bbbb", 4);

    var it = tree.range_reverse(.{ .unbounded = {} }, .{ .unbounded = {} });
    const e1 = it.next().?;
    try testing.expectEqualSlices(u8, "bbbb", e1.key);

    const e2 = it.next().?;
    try testing.expectEqualSlices(u8, "aaaaaaaa_gamma", e2.key);

    const e3 = it.next().?;
    try testing.expectEqualSlices(u8, "aaaaaaaa_beta", e3.key);

    const e4 = it.next().?;
    try testing.expectEqualSlices(u8, "aaaaaaaa_alpha", e4.key);

    try testing.expect(it.next() == null);
}

test "MassTree: reverse range many keys" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const N: u64 = 100;
    var i: u64 = 0;
    while (i < N) : (i += 1) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        _ = try tree.put(&buf, i);
    }

    var it = tree.range_reverse(.{ .unbounded = {} }, .{ .unbounded = {} });
    var count: usize = 0;
    var last_key: ?u64 = null;
    while (it.next()) |entry| {
        const k = std.mem.readInt(u64, entry.key[0..8], .big);
        if (last_key) |lk| {
            try testing.expect(k < lk);
        }
        last_key = k;
        count += 1;
    }
    try testing.expectEqual(@as(usize, @intCast(N)), count);
}

test "MassTree: forward and reverse yield same items" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const N: u64 = 50;
    var i: u64 = 0;
    while (i < N) : (i += 1) {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, i, .big);
        _ = try tree.put(&buf, i);
    }

    // Collect forward keys
    var forward_keys: [50]u64 = undefined;
    var fwd_count: usize = 0;
    var fwd_it = tree.range_all();
    while (fwd_it.next()) |entry| {
        forward_keys[fwd_count] = std.mem.readInt(u64, entry.key[0..8], .big);
        fwd_count += 1;
    }

    // Collect reverse keys
    var reverse_keys: [50]u64 = undefined;
    var rev_count: usize = 0;
    var rev_it = tree.range_reverse(.{ .unbounded = {} }, .{ .unbounded = {} });
    while (rev_it.next()) |entry| {
        reverse_keys[rev_count] = std.mem.readInt(u64, entry.key[0..8], .big);
        rev_count += 1;
    }

    try testing.expectEqual(fwd_count, rev_count);
    try testing.expectEqual(@as(usize, @intCast(N)), fwd_count);

    // Forward should equal reverse reversed
    for (0..fwd_count) |idx| {
        try testing.expectEqual(forward_keys[idx], reverse_keys[fwd_count - 1 - idx]);
    }
}

test "MassTree: range single key" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    _ = try tree.put("aaa", 1);
    _ = try tree.put("bbb", 2);
    _ = try tree.put("ccc", 3);

    var it = tree.range(.{ .included = "bbb" }, .{ .included = "bbb" });
    const e = it.next().?;
    try testing.expectEqualSlices(u8, "bbb", e.key);
    try testing.expectEqual(@as(u64, 2), e.value);
    try testing.expect(it.next() == null);
}

// ============================================================================
//  Concurrent Tests — Phase 4
// ============================================================================

/// Thread worker for concurrent disjoint insert tests.
/// Each thread inserts keys in its own non-overlapping range.
fn concurrent_insert_worker(tree_ptr: *MassTree(u64), tid: usize, keys_per_thread: usize) void {
    const base = tid * 1000;
    for (0..keys_per_thread) |i| {
        const key_val: u64 = @intCast(base + i);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, key_val, .big);
        _ = tree_ptr.put(&buf, key_val) catch return;
    }
}

test "MassTree concurrent: 2 threads disjoint inserts" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const NUM_THREADS = 2;
    const KEYS_PER_THREAD = 100;

    var threads: [NUM_THREADS]std.Thread = undefined;
    for (0..NUM_THREADS) |tid| {
        threads[tid] = try std.Thread.spawn(.{}, concurrent_insert_worker, .{ &tree, tid, KEYS_PER_THREAD });
    }
    for (&threads) |*t| t.join();

    try testing.expectEqual(@as(usize, NUM_THREADS * KEYS_PER_THREAD), tree.len());

    // Verify all keys present with correct values
    for (0..NUM_THREADS) |tid| {
        const base = tid * 1000;
        for (0..KEYS_PER_THREAD) |i| {
            const key_val: u64 = @intCast(base + i);
            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, key_val, .big);
            const val = tree.get(&buf);
            try testing.expect(val != null);
            try testing.expectEqual(key_val, val.?);
        }
    }
}

test "MassTree concurrent: 4 threads disjoint inserts" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const NUM_THREADS = 4;
    const KEYS_PER_THREAD = 100;

    var threads: [NUM_THREADS]std.Thread = undefined;
    for (0..NUM_THREADS) |tid| {
        threads[tid] = try std.Thread.spawn(.{}, concurrent_insert_worker, .{ &tree, tid, KEYS_PER_THREAD });
    }
    for (&threads) |*t| t.join();

    try testing.expectEqual(@as(usize, NUM_THREADS * KEYS_PER_THREAD), tree.len());

    // Verify all keys present
    for (0..NUM_THREADS) |tid| {
        const base = tid * 1000;
        for (0..KEYS_PER_THREAD) |i| {
            const key_val: u64 = @intCast(base + i);
            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, key_val, .big);
            try testing.expect(tree.get(&buf) != null);
        }
    }
}

/// Thread worker for overlapping key inserts.
/// All threads insert the same key range; last writer wins.
fn concurrent_overlap_worker(tree_ptr: *MassTree(u64), tid: usize, num_keys: usize) void {
    for (0..num_keys) |i| {
        const key_val: u64 = @intCast(i);
        const val: u64 = @intCast(tid * 1000 + i);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, key_val, .big);
        _ = tree_ptr.put(&buf, val) catch return;
    }
}

test "MassTree concurrent: 2 threads overlapping inserts" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const NUM_THREADS = 2;
    const NUM_KEYS = 100;

    var threads: [NUM_THREADS]std.Thread = undefined;
    for (0..NUM_THREADS) |tid| {
        threads[tid] = try std.Thread.spawn(.{}, concurrent_overlap_worker, .{ &tree, tid, NUM_KEYS });
    }
    for (&threads) |*t| t.join();

    // With overlapping keys, tree should have exactly NUM_KEYS entries
    try testing.expectEqual(@as(usize, NUM_KEYS), tree.len());

    // Verify all keys present (value is from whichever thread wrote last)
    for (0..NUM_KEYS) |i| {
        const key_val: u64 = @intCast(i);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, key_val, .big);
        try testing.expect(tree.get(&buf) != null);
    }
}

/// Reader thread: reads pre-populated keys and verifies they exist.
fn concurrent_reader_worker(tree_ptr: *const MassTree(u64), num_keys: usize, found_count: *std.atomic.Value(usize)) void {
    var count: usize = 0;
    for (0..num_keys) |i| {
        const key_val: u64 = @intCast(i);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, key_val, .big);
        if (tree_ptr.get(&buf) != null) {
            count += 1;
        }
    }
    _ = found_count.fetchAdd(count, .monotonic);
}

/// Writer thread: inserts new keys in a disjoint range.
fn concurrent_writer_worker(tree_ptr: *MassTree(u64), base: usize, num_keys: usize) void {
    for (0..num_keys) |i| {
        const key_val: u64 = @intCast(base + i);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, key_val, .big);
        _ = tree_ptr.put(&buf, key_val) catch return;
    }
}

test "MassTree concurrent: read + write (1 writer, 1 reader)" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    // Pre-populate 100 keys
    const PRE_POP = 100;
    for (0..PRE_POP) |i| {
        const key_val: u64 = @intCast(i);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, key_val, .big);
        _ = try tree.put(&buf, key_val);
    }

    try testing.expectEqual(@as(usize, PRE_POP), tree.len());

    var found_count = std.atomic.Value(usize).init(0);

    // Writer adds keys 1000..1099, reader reads keys 0..99
    const writer = try std.Thread.spawn(.{}, concurrent_writer_worker, .{ &tree, 1000, 100 });
    const reader = try std.Thread.spawn(.{}, concurrent_reader_worker, .{ @as(*const MassTree(u64), &tree), PRE_POP, &found_count });

    writer.join();
    reader.join();

    // All pre-populated keys should still be found by reader
    try testing.expectEqual(@as(usize, PRE_POP), found_count.load(.monotonic));
    // Final tree should have pre-populated + writer keys
    try testing.expectEqual(@as(usize, PRE_POP + 100), tree.len());
}

test "MassTree concurrent: inserts trigger splits (2 threads, 200 keys each)" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const NUM_THREADS = 2;
    const KEYS_PER_THREAD = 200;

    var threads: [NUM_THREADS]std.Thread = undefined;
    for (0..NUM_THREADS) |tid| {
        threads[tid] = try std.Thread.spawn(.{}, concurrent_insert_worker, .{ &tree, tid, KEYS_PER_THREAD });
    }
    for (&threads) |*t| t.join();

    try testing.expectEqual(@as(usize, NUM_THREADS * KEYS_PER_THREAD), tree.len());

    // Verify all keys with correct values
    for (0..NUM_THREADS) |tid| {
        const base = tid * 1000;
        for (0..KEYS_PER_THREAD) |i| {
            const key_val: u64 = @intCast(base + i);
            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, key_val, .big);
            const val = tree.get(&buf);
            try testing.expect(val != null);
            try testing.expectEqual(key_val, val.?);
        }
    }
}

/// Stress test worker: inserts keys and immediately reads them back.
fn stress_insert_verify_worker(tree_ptr: *MassTree(u64), tid: usize, keys_per_thread: usize, errors: *std.atomic.Value(usize)) void {
    const base = tid * 10000;
    for (0..keys_per_thread) |i| {
        const key_val: u64 = @intCast(base + i);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, key_val, .big);
        _ = tree_ptr.put(&buf, key_val) catch {
            _ = errors.fetchAdd(1, .monotonic);
            return;
        };
        // Immediate read-back verification
        if (tree_ptr.get(&buf)) |val| {
            if (val != key_val) {
                _ = errors.fetchAdd(1, .monotonic);
            }
        } else {
            _ = errors.fetchAdd(1, .monotonic);
        }
    }
}

test "MassTree concurrent: stress 4 threads × 500 keys with read-back" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const NUM_THREADS = 4;
    const KEYS_PER_THREAD = 500;

    var errors = std.atomic.Value(usize).init(0);

    var threads: [NUM_THREADS]std.Thread = undefined;
    for (0..NUM_THREADS) |tid| {
        threads[tid] = try std.Thread.spawn(.{}, stress_insert_verify_worker, .{ &tree, tid, KEYS_PER_THREAD, &errors });
    }
    for (&threads) |*t| t.join();

    try testing.expectEqual(@as(usize, 0), errors.load(.monotonic));
    try testing.expectEqual(@as(usize, NUM_THREADS * KEYS_PER_THREAD), tree.len());

    // Full verification pass
    for (0..NUM_THREADS) |tid| {
        const base = tid * 10000;
        for (0..KEYS_PER_THREAD) |i| {
            const key_val: u64 = @intCast(base + i);
            var buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &buf, key_val, .big);
            const val = tree.get(&buf);
            try testing.expect(val != null);
            try testing.expectEqual(key_val, val.?);
        }
    }
}

/// Long key worker: inserts 24-byte keys spanning multiple trie layers.
fn concurrent_long_key_worker(tree_ptr: *MassTree(u64), tid: usize, num_keys: usize) void {
    for (0..num_keys) |i| {
        // Create a 24-byte key like "thread_XX_key_XXXXXXXX"
        var buf: [24]u8 = [_]u8{0} ** 24;
        _ = std.fmt.bufPrint(&buf, "thread_{d:0>2}_key_{d:0>8}", .{ tid, i }) catch return;
        const val: u64 = @intCast(tid * 1000 + i);
        _ = tree_ptr.put(&buf, val) catch return;
    }
}

test "MassTree concurrent: long keys multi-layer (4 threads)" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const NUM_THREADS = 4;
    const KEYS_PER_THREAD = 50;

    var threads: [NUM_THREADS]std.Thread = undefined;
    for (0..NUM_THREADS) |tid| {
        threads[tid] = try std.Thread.spawn(.{}, concurrent_long_key_worker, .{ &tree, tid, KEYS_PER_THREAD });
    }
    for (&threads) |*t| t.join();

    try testing.expectEqual(@as(usize, NUM_THREADS * KEYS_PER_THREAD), tree.len());

    // Verify all keys
    for (0..NUM_THREADS) |tid| {
        for (0..KEYS_PER_THREAD) |i| {
            var buf: [24]u8 = [_]u8{0} ** 24;
            _ = std.fmt.bufPrint(&buf, "thread_{d:0>2}_key_{d:0>8}", .{ tid, i }) catch continue;
            const expected: u64 = @intCast(tid * 1000 + i);
            const val = tree.get(&buf);
            try testing.expect(val != null);
            try testing.expectEqual(expected, val.?);
        }
    }
}

/// Sequential key worker: threads insert interleaved sequential keys.
fn concurrent_sequential_worker(tree_ptr: *MassTree(u64), tid: usize, num_threads: usize, total_keys: usize) void {
    var i: usize = tid;
    while (i < total_keys) : (i += num_threads) {
        const key_val: u64 = @intCast(i);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, key_val, .big);
        _ = tree_ptr.put(&buf, key_val) catch return;
    }
}

test "MassTree concurrent: interleaved sequential keys (high split contention)" {
    var tree = try MassTree(u64).init(testing.allocator);
    defer tree.deinit();

    const NUM_THREADS = 4;
    const TOTAL_KEYS = 1000;

    var threads: [NUM_THREADS]std.Thread = undefined;
    for (0..NUM_THREADS) |tid| {
        threads[tid] = try std.Thread.spawn(.{}, concurrent_sequential_worker, .{ &tree, tid, NUM_THREADS, TOTAL_KEYS });
    }
    for (&threads) |*t| t.join();

    try testing.expectEqual(@as(usize, TOTAL_KEYS), tree.len());

    // Verify all keys
    for (0..TOTAL_KEYS) |i| {
        const key_val: u64 = @intCast(i);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, key_val, .big);
        const val = tree.get(&buf);
        try testing.expect(val != null);
        try testing.expectEqual(key_val, val.?);
    }
}
