//! Compile-time configuration constants for the Masstree.
//!
//! ## FANOUT
//!
//! Controls the maximum number of keys stored in a single B⁺ tree node
//! (both leaf and interior).  A value of 15 is chosen so that a leaf
//! node's hot data fits comfortably in a few cache lines, keeping the
//! linear scan within a node fast.
//!
//! ## KEY_SLICE_LEN
//!
//! The number of bytes consumed per trie layer.  The Masstree design
//! fixes this at 8, mapping each slice to a `u64` for cheap integer
//! comparison instead of byte-by-byte `memcmp`.

/// Maximum number of keys per B⁺ tree node (leaf or interior).
pub const FANOUT: usize = 15;

/// Bytes consumed by each trie layer.
pub const KEY_SLICE_LEN: usize = 8;

// ============================================================================
//  Tagged Pointer Utilities (Phase 4 — Concurrency)
// ============================================================================

/// Opaque pointer info extracted from a tagged pointer.
pub const TaggedPtr = struct {
    ptr: *anyopaque,
    is_leaf: bool,
};

/// Pack a pointer and an is_leaf flag into a single usize.
///
/// The low bit stores is_leaf (safe because node allocations are
/// at least 8-byte aligned, leaving bits 0-2 free for tagging).
pub fn tag_ptr(ptr: *anyopaque, is_leaf: bool) usize {
    return @intFromPtr(ptr) | @as(usize, @intFromBool(is_leaf));
}

/// Unpack a tagged pointer into the raw pointer and is_leaf flag.
pub fn untag_ptr(tagged: usize) TaggedPtr {
    return .{
        .ptr = @ptrFromInt(tagged & ~@as(usize, 1)),
        .is_leaf = (tagged & 1) != 0,
    };
}

/// Get just the raw pointer from a tagged value.
pub fn untag_raw(tagged: usize) *anyopaque {
    return @ptrFromInt(tagged & ~@as(usize, 1));
}

/// Check the is_leaf bit of a tagged value.
pub fn is_leaf_tagged(tagged: usize) bool {
    return (tagged & 1) != 0;
}
