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
