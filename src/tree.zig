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

        /// Opaque root pointer — either a *Leaf or *InternodeNode.
        root: *anyopaque,

        /// Whether the root is currently a leaf node.
        root_is_leaf: bool,

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
                .root = @ptrCast(leaf),
                .root_is_leaf = true,
                .count = 0,
                .allocator = allocator,
            };
        }

        /// Destroy the tree, freeing all nodes recursively.
        pub fn deinit(self: *Self) void {
            self.destroy_node(self.root, self.root_is_leaf);
            self.root = undefined;
            self.root_is_leaf = true;
            self.count = 0;
        }

        /// Return the number of key-value pairs.
        pub fn len(self: *const Self) usize {
            return self.count;
        }

        /// Check if the tree is empty.
        pub fn is_empty(self: *const Self) bool {
            return self.count == 0;
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
            return ForwardIterator.init(self.root, self.root_is_leaf, start, end);
        }

        /// Create a reverse iterator over keys in [start, end] (descending order).
        pub fn range_reverse(self: *const Self, start: RangeBound, end: RangeBound) ReverseIterator {
            return ReverseIterator.init(self.root, self.root_is_leaf, start, end);
        }

        /// Create a forward iterator over all keys.
        pub fn range_all(self: *const Self) ForwardIterator {
            return self.range(.{ .unbounded = {} }, .{ .unbounded = {} });
        }

        // ====================================================================
        //  GET
        // ====================================================================

        /// Look up a key and return its value, or null if not found.
        pub fn get(self: *const Self, key_bytes: []const u8) ?V {
            var k = Key.init(key_bytes);
            var node_ptr: *anyopaque = self.root;
            var is_leaf = self.root_is_leaf;

            // Layer loop: descend through trie layers
            while (true) {
                const leaf = navigate_to_leaf(node_ptr, is_leaf, &k);
                const result = search_leaf_for_get(leaf, &k);

                switch (result) {
                    .found => |slot| return leaf.get_value(slot),
                    .layer => |slot| {
                        const layer_ptr = leaf.get_layer(slot) orelse return null;
                        k.shift();
                        node_ptr = layer_ptr;
                        is_leaf = true; // layer roots start as leaves
                        continue;
                    },
                    .not_found => return null,
                }
            }
        }

        // ====================================================================
        //  PUT (insert or update)
        // ====================================================================

        /// Insert or update a key-value pair.
        /// Returns the old value if the key already existed.
        pub fn put(self: *Self, key_bytes: []const u8, val: V) Allocator.Error!?V {
            var k = Key.init(key_bytes);
            return self.put_at_layer(&k, val, self.root, self.root_is_leaf, true);
        }

        /// Internal recursive put, handles layer descent and split propagation.
        fn put_at_layer(
            self: *Self,
            k: *Key,
            val: V,
            layer_root: *anyopaque,
            layer_root_is_leaf: bool,
            is_main_root: bool,
        ) Allocator.Error!?V {
            const leaf = navigate_to_leaf_mut(layer_root, layer_root_is_leaf, k);
            const search = search_leaf_for_insert(leaf, k);

            switch (search) {
                .found => |slot| {
                    const old = leaf.get_value(slot);
                    leaf.set_value(slot, val);
                    return old;
                },
                .not_found => |logical_pos| {
                    if (!leaf.is_full()) {
                        _ = try leaf.insert_key(logical_pos, k.*, LV.init_value(val));
                        self.count += 1;
                        return null;
                    }
                    return self.handle_split_and_insert(
                        leaf,
                        logical_pos,
                        k,
                        val,
                        layer_root,
                        is_main_root,
                    );
                },
                .conflict => |slot| {
                    try self.create_layer(leaf, slot, k, val);
                    self.count += 1;
                    return null;
                },
                .layer => |slot| {
                    const layer_ptr = leaf.get_layer(slot) orelse
                        return error.OutOfMemory;
                    k.shift();
                    return self.put_at_layer(k, val, layer_ptr, true, false);
                },
            }
        }

        // ====================================================================
        //  REMOVE
        // ====================================================================

        /// Remove a key from the tree.
        /// Returns the removed value, or null if the key didn't exist.
        pub fn remove(self: *Self, key_bytes: []const u8) ?V {
            var k = Key.init(key_bytes);
            return self.remove_at_layer(&k, self.root, self.root_is_leaf);
        }

        /// Internal recursive remove.
        fn remove_at_layer(self: *Self, k: *Key, layer_root: *anyopaque, layer_is_leaf: bool) ?V {
            const leaf = navigate_to_leaf_mut(layer_root, layer_is_leaf, k);
            const result = search_leaf_for_get(
                @as(*const Leaf, @ptrCast(@alignCast(leaf))),
                k,
            );

            switch (result) {
                .found => |slot| {
                    const old_val = leaf.get_value(slot);
                    leaf.remove_slot_entry(slot);
                    self.count -= 1;
                    return old_val;
                },
                .layer => |slot| {
                    const layer_ptr = leaf.get_layer(slot) orelse return null;
                    k.shift();
                    return self.remove_at_layer(k, layer_ptr, true);
                },
                .not_found => return null,
            }
        }

        // ====================================================================
        //  Tree Traversal
        // ====================================================================

        /// Navigate from a root pointer down to the leaf (const).
        /// Uses `is_leaf` flag and `internode.height` to determine node types
        /// instead of reading `NodeVersion.is_leaf()` from raw pointers.
        fn navigate_to_leaf(root: *anyopaque, root_is_leaf: bool, k: *const Key) *const Leaf {
            if (root_is_leaf) {
                return @ptrCast(@alignCast(root));
            }

            var inode: *const InternodeNode = @ptrCast(@alignCast(root));
            while (true) {
                const child_idx = inode.upper_bound(k.ikey());
                const node = inode.get_child(child_idx) orelse unreachable;

                if (inode.height == 0) {
                    // Children are leaves
                    return @ptrCast(@alignCast(node));
                }

                // Children are internodes
                inode = @ptrCast(@alignCast(node));
            }
        }

        /// Navigate from a root pointer down to the leaf (mutable).
        fn navigate_to_leaf_mut(root: *anyopaque, root_is_leaf: bool, k: *const Key) *Leaf {
            if (root_is_leaf) {
                return @ptrCast(@alignCast(root));
            }

            var inode: *const InternodeNode = @ptrCast(@alignCast(root));
            while (true) {
                const child_idx = inode.upper_bound(k.ikey());
                const node = inode.get_child(child_idx) orelse unreachable;

                if (inode.height == 0) {
                    return @ptrCast(@alignCast(node));
                }

                inode = @ptrCast(@alignCast(node));
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
        //  Split Handling
        // ====================================================================

        /// Handle a leaf split and insert.
        fn handle_split_and_insert(
            self: *Self,
            leaf: *Leaf,
            logical_pos: usize,
            k: *const Key,
            val: V,
            layer_root: *anyopaque,
            is_main_root: bool,
        ) Allocator.Error!?V {
            const ik = k.ikey();
            const klx: u8 = if (k.has_suffix()) KSUF_KEYLENX else @intCast(k.current_len());
            const suf: ?[]const u8 = if (k.has_suffix()) k.suffix() else null;

            const sp = leaf.calculate_split_point(logical_pos, ik) orelse
                return error.OutOfMemory;

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

            self.count += 1;
            return null;
        }

        /// Propagate a split upward through internodes.
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

                if (!parent.is_full()) {
                    const child_idx = parent.find_child_index(left) orelse 0;
                    parent.insert_key_and_child(child_idx, s_ikey, right);
                    set_node_parent(right, @ptrCast(parent), leaf_level);
                    return;
                }

                // Parent full — split it
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

                left = @ptrCast(parent);
                right = @ptrCast(new_sibling);
                s_ikey = split_result.popup_key;
                ly_root = ly_root;
                main_root = (left == self.root);
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
                self.root = @ptrCast(new_root);
                self.root_is_leaf = false;
            } else {
                self.update_layer_root_pointer(layer_root, @ptrCast(new_root));
            }
        }

        /// When a sublayer root splits and creates a new root, find the
        /// parent leaf slot that points to the old layer_root and update it.
        fn update_layer_root_pointer(self: *Self, old_root: *anyopaque, new_root: *anyopaque) void {
            self.find_and_replace_layer_ptr(self.root, self.root_is_leaf, old_root, new_root);
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
                        if (leaf.values[slot].try_as_layer()) |layer_ptr| {
                            if (layer_ptr == old_ptr) {
                                leaf.values[slot] = LV.init_layer(new_ptr);
                                return;
                            }
                            // Recurse into sublayer (layer roots are always leaves initially,
                            // but can become internodes — pass true as they start as leaves)
                            self.find_and_replace_layer_ptr(layer_ptr, true, old_ptr, new_ptr);
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
                    pt.values[prev_twig_slot] = LV.init_layer(@ptrCast(twig));
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
                pt.values[prev_twig_slot] = LV.init_layer(@ptrCast(final_leaf));
            } else {
                layer_root_ptr = @ptrCast(final_leaf);
            }

            parent_leaf.make_layer(conflict_slot, layer_root_ptr.?);
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
                        if (leaf.values[slot].try_as_layer()) |layer_ptr| {
                            self.destroy_node(layer_ptr, true); // layers start as leaves
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
