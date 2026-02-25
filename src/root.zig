//! Public re-export module for the **masstree-zig** library.
//!
//! ## Phase 1 — Core Data Structures
//!
//! This module re-exports the Phase 1 core types: keys, permuter,
//! node version, suffix storage, values, leaf nodes, and interior nodes.
//!
//! Tree-level operations (get, put, remove, range scan) will be added
//! in Phase 2.

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

test {
    // Pull in tests from every module so `zig build test` on root.zig
    // runs the full inline-test suite.
    const t = @import("std").testing;
    t.refAllDeclsRecursive(@This());
}
