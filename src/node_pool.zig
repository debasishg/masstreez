//! Thread-local, size-class node pools for leaf and internode allocation.
//!
//! ## Design
//!
//! Nodes are bucketed by cache-line count (1–`MAX_SIZE_CLASSES`), enabling
//! cross-type reuse within the same size class.  Each bucket is an
//! **intrusive freelist** — the first 8 bytes of each freed block store
//! the next pointer.
//!
//! The pool is `threadlocal`, so alloc/dealloc are lock-free.  A thread
//! that reclaims a node via EBR returns it to **its own** pool, which
//! may differ from the allocating thread's pool — this is intentional
//! (matches the Rust masstree design).
//!
//! ## Constants
//!
//! | Constant          | Value | Meaning |
//! |-------------------|-------|---------|
//! | `CACHE_LINE`      | 64    | Bucket granularity in bytes |
//! | `MAX_SIZE_CLASSES` | 20   | Max bucket index (nodes up to 1280 bytes) |
//! | `POOL_CAPACITY`   | 512   | Max cached blocks per bucket per thread |
//!
//! ## Teardown Bypass
//!
//! During `MassTree.deinit()`, the dropping thread frees every node but
//! never allocates.  Caching would pointlessly fill the bucket, so
//! `teardown_dealloc_*` bypasses the pool and frees directly.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const CACHE_LINE: usize = 64;
pub const MAX_SIZE_CLASSES: usize = 20;
pub const POOL_CAPACITY: usize = 512;

// ============================================================================
//  Intrusive Freelist
// ============================================================================

/// A free block's header (first 8 bytes repurposed as a next pointer).
const FreeNode = struct {
    next: ?*FreeNode,
};

/// Per-size-class intrusive freelist.
const Freelist = struct {
    head: ?*FreeNode = null,
    count: usize = 0,

    /// Pop one block from the freelist.  Returns a raw pointer or null.
    fn pop(self: *Freelist) ?[*]u8 {
        const node = self.head orelse return null;
        self.head = node.next;
        self.count -= 1;
        return @ptrCast(node);
    }

    /// Push a block back onto the freelist.
    /// Returns `true` if accepted, `false` if the pool is full (caller
    /// should free directly via the backing allocator).
    fn push(self: *Freelist, ptr: [*]u8) bool {
        if (self.count >= POOL_CAPACITY) return false;
        const node: *FreeNode = @ptrCast(@alignCast(ptr));
        node.next = self.head;
        self.head = node;
        self.count += 1;
        return true;
    }

    /// Drain all cached blocks, freeing them via the backing allocator.
    fn drain(self: *Freelist, comptime size_class: usize, allocator: Allocator) void {
        const alloc_bytes = size_class * CACHE_LINE;
        while (self.head) |node| {
            self.head = node.next;
            self.count -= 1;
            const raw: [*]u8 = @ptrCast(node);
            const slice = raw[0..alloc_bytes];
            allocator.free(@alignCast(slice));
        }
    }
};

// ============================================================================
//  ThreadPool
// ============================================================================

/// Per-thread collection of freelists, one per size class.
const ThreadPool = struct {
    lists: [MAX_SIZE_CLASSES]Freelist = [_]Freelist{.{}} ** MAX_SIZE_CLASSES,
};

threadlocal var tls_pool: ThreadPool = .{};

// ============================================================================
//  Public API — comptime-generic over the node type
// ============================================================================

/// Allocate a node from the thread-local pool (fast path) or from the
/// backing allocator (slow path).
///
/// Returns an uninitialized pointer — caller must write all fields.
pub fn pool_alloc(comptime T: type, allocator: Allocator) Allocator.Error!*T {
    const size = @sizeOf(T);
    const nl = comptime (size + CACHE_LINE - 1) / CACHE_LINE;

    if (comptime nl <= MAX_SIZE_CLASSES) {
        if (tls_pool.lists[nl - 1].pop()) |raw| {
            return @ptrCast(@alignCast(raw));
        }
    }
    return allocator.create(T);
}

/// Return a node to the thread-local pool (fast path) or free it via
/// the backing allocator (slow path, pool full).
pub fn pool_dealloc(comptime T: type, ptr: *T, allocator: Allocator) void {
    const size = @sizeOf(T);
    const nl = comptime (size + CACHE_LINE - 1) / CACHE_LINE;

    if (comptime nl <= MAX_SIZE_CLASSES) {
        if (tls_pool.lists[nl - 1].push(@ptrCast(ptr))) return;
    }
    allocator.destroy(ptr);
}

/// Bypass the pool and free directly.  Use during tree teardown.
pub fn teardown_dealloc(comptime T: type, ptr: *T, allocator: Allocator) void {
    allocator.destroy(ptr);
}

// ============================================================================
//  Reclaimer function pointers (for EBR defer_retire)
// ============================================================================

/// Build a reclamation function for leaf nodes of a given value type.
///
/// The returned function pointer has the signature expected by
/// `Guard.defer_retire()`: `fn (*anyopaque, Allocator) void`.
pub fn make_leaf_reclaimer(comptime V: type) *const fn (*anyopaque, Allocator) void {
    const leaf_mod = @import("leaf.zig");
    const Leaf = leaf_mod.LeafNode(V);

    return &struct {
        fn reclaim(ptr: *anyopaque, allocator: Allocator) void {
            const leaf: *Leaf = @ptrCast(@alignCast(ptr));
            leaf.suffix.deinit();
            pool_dealloc(Leaf, leaf, allocator);
        }
    }.reclaim;
}

/// Reclaim function for internode nodes.
pub fn reclaim_internode(ptr: *anyopaque, allocator: Allocator) void {
    const interior_mod = @import("interior.zig");
    const InternodeNode = interior_mod.InternodeNode;
    const inode: *InternodeNode = @ptrCast(@alignCast(ptr));
    pool_dealloc(InternodeNode, inode, allocator);
}

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "NodePool: alloc and dealloc leaf" {
    const leaf_mod = @import("leaf.zig");
    const Leaf = leaf_mod.LeafNode(u64);

    const ptr = try pool_alloc(Leaf, testing.allocator);
    pool_dealloc(Leaf, ptr, testing.allocator);

    // Second alloc should hit the pool.
    const ptr2 = try pool_alloc(Leaf, testing.allocator);
    try testing.expect(ptr == ptr2);

    // Free directly to avoid test-allocator leak.
    teardown_dealloc(Leaf, ptr2, testing.allocator);
}

test "NodePool: alloc and dealloc internode" {
    const interior_mod = @import("interior.zig");
    const Inode = interior_mod.InternodeNode;

    const ptr = try pool_alloc(Inode, testing.allocator);
    pool_dealloc(Inode, ptr, testing.allocator);

    const ptr2 = try pool_alloc(Inode, testing.allocator);
    try testing.expect(ptr == ptr2);

    // Free directly to avoid test-allocator leak.
    teardown_dealloc(Inode, ptr2, testing.allocator);
}

test "NodePool: teardown_dealloc bypasses pool" {
    const interior_mod = @import("interior.zig");
    const Inode = interior_mod.InternodeNode;

    const ptr = try pool_alloc(Inode, testing.allocator);
    teardown_dealloc(Inode, ptr, testing.allocator);

    // Pool should be empty — next alloc goes to the backing allocator.
    const nl = comptime (@sizeOf(Inode) + CACHE_LINE - 1) / CACHE_LINE;
    try testing.expectEqual(@as(usize, 0), tls_pool.lists[nl - 1].count);
}
