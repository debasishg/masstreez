//! Prefetch utilities for Masstree node access.
//!
//! Provides cross-platform prefetch wrappers around Zig's `@prefetch`
//! builtin, following the same strategy as the Rust implementation:
//!
//! - **Internode descent:** prefetch the second cache line (offset +64)
//!   of an internode before searching its keys, then prefetch the
//!   selected child while validating the parent version.
//! - **B-link traversal:** prefetch the successor leaf two hops ahead
//!   to hide L3 latency (~50-100ns).
//! - **Value access:** prefetch value data before returning from a leaf
//!   search.
//!
//! All prefetch calls are architectural no-ops for null or invalid
//! addresses, so no null checks are needed before calling.
//!
//! ## Feature Gate
//!
//! Set `enable_prefetch = false` at comptime to disable all prefetching
//! (useful for A/B performance comparison).

const std = @import("std");

/// Cache line size in bytes. Matches the Rust/C++ masstree constant.
pub const CACHE_LINE: usize = 64;

/// Master switch for prefetching. Set to `false` to make all
/// prefetch calls compile to nothing (for benchmarking comparisons).
pub const enable_prefetch: bool = true;

// ============================================================================
//  Core Prefetch Operations
// ============================================================================

/// Prefetch data for reading at the given pointer.
///
/// Uses temporal locality hint = 3 (T0: all cache levels) to keep the
/// data in L1. This is equivalent to `_mm_prefetch(ptr, _MM_HINT_T0)`
/// on x86 and `prfm pldl1keep` on AArch64.
///
/// No-op if `ptr` is null or `enable_prefetch` is `false`.
pub inline fn prefetch_read(ptr: ?*const anyopaque) void {
    if (!enable_prefetch) return;
    const p = ptr orelse return;
    const byte_ptr: [*]const u8 = @ptrCast(p);
    @prefetch(byte_ptr, .{ .rw = .read, .locality = 3, .cache = .data });
}

/// Prefetch data for writing at the given pointer.
///
/// Uses temporal locality hint = 3 with write intent. On x86 this
/// maps to `_MM_HINT_ET0` which requests the cache line in exclusive
/// state, avoiding the shared→exclusive upgrade penalty.
///
/// No-op if `ptr` is null or `enable_prefetch` is `false`.
pub inline fn prefetch_write(ptr: ?*anyopaque) void {
    if (!enable_prefetch) return;
    const p = ptr orelse return;
    const byte_ptr: [*]u8 = @ptrCast(p);
    @prefetch(byte_ptr, .{ .rw = .write, .locality = 3, .cache = .data });
}

/// Prefetch a specific byte offset from a base pointer (read).
///
/// Used to prefetch the second cache line of an internode, e.g.:
///   `prefetch_read_offset(inode_ptr, 64)`
///
/// No-op if `ptr` is null or `enable_prefetch` is `false`.
pub inline fn prefetch_read_offset(ptr: ?*const anyopaque, offset: usize) void {
    if (!enable_prefetch) return;
    const p = ptr orelse return;
    const base: [*]const u8 = @ptrCast(p);
    @prefetch(base + offset, .{ .rw = .read, .locality = 3, .cache = .data });
}

// ============================================================================
//  Internode Prefetch Helpers
// ============================================================================

/// Prefetch an internode's second cache line before key search.
///
/// Cache line 0 (bytes 0-63) contains the version word and first ~7
/// ikeys — likely already hot from the version check. Cache line 1
/// (bytes 64-127) contains the remaining ikeys and is prefetched here
/// to hide L2 latency during the linear/binary search.
pub inline fn prefetch_internode_keys(inode_ptr: *const anyopaque) void {
    prefetch_read_offset(inode_ptr, CACHE_LINE);
}

/// Prefetch a child node pointer before version validation.
///
/// After selecting a child during internode descent, prefetch the child
/// node while we're still validating the parent version. This overlaps
/// the version check computation with the child's memory fetch.
pub inline fn prefetch_child(child_ptr: ?*anyopaque) void {
    prefetch_read(child_ptr);
}

/// Prefetch for speculative grandchild access during internode chains.
///
/// When the child is also an internode, prefetch its second cache line
/// (ikeys) and its first child pointer speculatively.
pub inline fn prefetch_grandchild(child_ptr: *const anyopaque, comptime InternodeNode: type) void {
    if (!enable_prefetch) return;
    const child_inode: *const InternodeNode = @ptrCast(@alignCast(child_ptr));
    // Prefetch CL1 of child internode (remaining ikeys)
    prefetch_read_offset(child_ptr, CACHE_LINE);
    // Speculatively prefetch the first grandchild
    if (child_inode.children[0]) |grandchild| {
        prefetch_read(grandchild);
    }
}

// ============================================================================
//  Leaf / B-link Prefetch Helpers
// ============================================================================

/// Prefetch a leaf node for reading.
///
/// Prefetches cache line 0 (version + permutation + first ikeys) and
/// cache line 1 (remaining ikeys + keylenx) of the leaf.
pub inline fn prefetch_leaf_read(leaf_ptr: ?*const anyopaque) void {
    prefetch_read(leaf_ptr);
    if (leaf_ptr) |p| {
        prefetch_read_offset(p, CACHE_LINE);
    }
}

/// Prefetch a leaf node for writing (before locking).
///
/// Requests exclusive cache line ownership to avoid the shared→exclusive
/// upgrade penalty when the lock is acquired.
pub inline fn prefetch_leaf_write(leaf_ptr: ?*anyopaque) void {
    prefetch_write(leaf_ptr);
}

/// Prefetch B-link successor for read-ahead during forward traversal.
///
/// Prefetches two cache lines of the next-next leaf to hide L3 latency
/// (~50-100ns) while we're comparing the ikey_bound of the current successor.
pub inline fn prefetch_blink_ahead(next_next_ptr: ?*const anyopaque) void {
    prefetch_read(next_next_ptr);
    if (next_next_ptr) |p| {
        prefetch_read_offset(p, CACHE_LINE);
    }
}

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "prefetch: read does not crash on valid pointer" {
    var x: u64 = 42;
    prefetch_read(&x);
    try testing.expectEqual(@as(u64, 42), x);
}

test "prefetch: read handles null" {
    prefetch_read(null);
}

test "prefetch: write does not crash on valid pointer" {
    var x: u64 = 42;
    prefetch_write(@ptrCast(&x));
    try testing.expectEqual(@as(u64, 42), x);
}

test "prefetch: write handles null" {
    prefetch_write(null);
}

test "prefetch: read_offset does not crash" {
    var buf: [256]u8 = [_]u8{0xAB} ** 256;
    prefetch_read_offset(&buf, 64);
    prefetch_read_offset(&buf, 128);
    try testing.expectEqual(@as(u8, 0xAB), buf[0]);
}

test "prefetch: leaf read prefetches two cache lines" {
    var buf: [256]u8 = [_]u8{0} ** 256;
    prefetch_leaf_read(&buf);
    try testing.expectEqual(@as(u8, 0), buf[0]);
}

test "prefetch: blink ahead handles null" {
    prefetch_blink_ahead(null);
}
