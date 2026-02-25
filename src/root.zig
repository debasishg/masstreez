//! Public re-export module for the **masstree-zig** library.
//!
//! ## Phase 1 — Core Data Structures
//! ## Phase 2 — Single-Threaded Tree Operations
//! ## Phase 3 — Range Scan
//!
//! This module re-exports core types and the MassTree API.

// -- Core types --
pub const key = @import("key.zig");
pub const config = @import("config.zig");

// -- Node infrastructure --
pub const permuter = @import("permuter.zig");
pub const node_version = @import("node_version.zig");
pub const suffix = @import("suffix.zig");
pub const value = @import("value.zig");

// -- Node types --
pub const leaf = @import("leaf.zig");
pub const interior = @import("interior.zig");

// -- Tree --
pub const tree = @import("tree.zig");
pub const range = @import("range.zig");

// -- Convenience re-exports --
pub const Key = key.Key;
pub const Permuter15 = permuter.Permuter15;
pub const NodeVersion = node_version.NodeVersion;
pub const LockGuard = node_version.LockGuard;
pub const SuffixBag = suffix.SuffixBag;
pub const InternodeNode = interior.InternodeNode;

/// Leaf node type parameterized over value type V.
pub fn LeafNode(comptime V: type) type {
    return leaf.LeafNode(V);
}

/// Leaf value type parameterized over V.
pub fn LeafValue(comptime V: type) type {
    return value.LeafValue(V);
}

/// MassTree type parameterized over value type V.
pub fn MassTree(comptime V: type) type {
    return tree.MassTree(V);
}

/// Range bound specification.
pub const RangeBound = range.RangeBound;

test {
    // Pull in tests from every module so `zig build test` on root.zig
    // runs the full inline-test suite.
    const t = @import("std").testing;
    t.refAllDeclsRecursive(@This());
}
