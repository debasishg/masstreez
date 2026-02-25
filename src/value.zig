//! Value types for leaf node storage.
//!
//! This module provides the value types used in leaf nodes:
//! - `LeafValue(V)`: Comptime-generic tagged union for values, empty slots, and layer pointers
//!
//! ## Memory Ownership
//!
//! In the current single-threaded phase, values are stored directly.
//! Phase 5 may introduce reference counting for concurrent reads.
//!
//! ## Layer Pointers (Phase 4)
//!
//! Layer pointers are stored as tagged `usize` values (low bit = is_leaf)
//! so that the node type can be determined without casting. This fixes
//! the sublayer root bug where a layer root becomes an internode after
//! splits but was always assumed to be a leaf.

const std = @import("std");
const config = @import("config.zig");

/// Re-export for use by leaf.zig and other modules.
pub const TaggedLayerPtr = config.TaggedPtr;

// ============================================================================
//  LeafValue(V) — comptime-generic tagged union
// ============================================================================

/// Value stored in a leaf slot.
///
/// A leaf node's value array holds one `LeafValue(V)` per slot.
/// Each slot can be empty, hold a value, or point to a next-layer subtree.
pub fn LeafValue(comptime V: type) type {
    return union(enum) {
        /// Slot is empty (no key assigned).
        empty: void,

        /// Slot contains a value of type V.
        value: V,

        /// Slot contains a pointer to a next-layer subtree, stored as a
        /// tagged usize (low bit = is_leaf). Use `try_as_layer()` to
        /// extract the pointer and node-type flag.
        layer: usize,

        const Self = @This();

        /// Create an empty leaf value.
        pub fn init_empty() Self {
            return .{ .empty = {} };
        }

        /// Create a leaf value containing a value.
        pub fn init_value(v: V) Self {
            return .{ .value = v };
        }

        /// Create a leaf value containing a layer pointer.
        /// `is_leaf` indicates whether the layer root is a leaf node.
        pub fn init_layer(ptr: *anyopaque, is_leaf: bool) Self {
            return .{ .layer = config.tag_ptr(ptr, is_leaf) };
        }

        /// Check if this slot is empty.
        pub fn is_empty(self: Self) bool {
            return self == .empty;
        }

        /// Check if this slot contains a value.
        pub fn is_value(self: Self) bool {
            return self == .value;
        }

        /// Check if this slot contains a layer pointer.
        pub fn is_layer(self: Self) bool {
            return self == .layer;
        }

        /// Get the value, or null if not a value variant.
        pub fn try_as_value(self: Self) ?V {
            return switch (self) {
                .value => |v| v,
                else => null,
            };
        }

        /// Get the value.
        /// Panics (safety-checked unreachable) if not a value variant.
        pub fn as_value(self: Self) V {
            return switch (self) {
                .value => |v| v,
                else => unreachable,
            };
        }

        /// Get a pointer to the value for in-place mutation.
        /// Returns null if not a value variant.
        pub fn try_as_value_ptr(self: *Self) ?*V {
            return switch (self.*) {
                .value => &self.value,
                else => null,
            };
        }

        /// Get the layer pointer and node type, or null if not a layer variant.
        pub fn try_as_layer(self: Self) ?config.TaggedPtr {
            return switch (self) {
                .layer => |tagged| config.untag_ptr(tagged),
                else => null,
            };
        }

        /// Get the layer pointer and node type.
        /// Panics (safety-checked unreachable) if not a Layer variant.
        pub fn as_layer(self: Self) config.TaggedPtr {
            return switch (self) {
                .layer => |tagged| config.untag_ptr(tagged),
                else => unreachable,
            };
        }

        /// Format for debug output.
        pub fn format(
            self: Self,
            comptime _: []const u8,
            _: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            switch (self) {
                .empty => try writer.writeAll("LeafValue.empty"),
                .value => |v| {
                    if (comptime std.meta.hasFn(V, "format")) {
                        try writer.print("LeafValue.value({any})", .{v});
                    } else {
                        try writer.writeAll("LeafValue.value(...)");
                    }
                },
                .layer => |tagged| {
                    const info = config.untag_ptr(tagged);
                    try writer.print("LeafValue.layer(0x{x}, is_leaf={})", .{ @intFromPtr(info.ptr), info.is_leaf });
                },
            }
        }
    };
}

// ============================================================================
//  Split Types
// ============================================================================

/// Split point for leaf node splitting.
pub const SplitPoint = struct {
    /// Logical position where to split (in post-insert coordinates).
    pos: usize,

    /// The ikey that will be the first key of the new (right) leaf.
    split_ikey: u64,
};

/// Which leaf to insert into after a split.
pub const InsertTarget = enum {
    /// Insert into the original (left) leaf.
    left,

    /// Insert into the new (right) leaf.
    right,
};

// ============================================================================
//  Tests
// ============================================================================

const testing = std.testing;

test "LeafValue: empty" {
    const LV = LeafValue(u64);
    const v = LV.init_empty();
    try testing.expect(v.is_empty());
    try testing.expect(!v.is_value());
    try testing.expect(!v.is_layer());
    try testing.expect(v.try_as_value() == null);
}

test "LeafValue: value" {
    const LV = LeafValue(u64);
    const v = LV.init_value(42);
    try testing.expect(!v.is_empty());
    try testing.expect(v.is_value());
    try testing.expect(!v.is_layer());
    try testing.expectEqual(@as(u64, 42), v.as_value());
    try testing.expectEqual(@as(u64, 42), v.try_as_value().?);
}

test "LeafValue: layer" {
    const LV = LeafValue(u64);
    var dummy: u64 = 0xDEAD;
    const ptr: *anyopaque = @ptrCast(&dummy);
    const v = LV.init_layer(ptr, true);
    try testing.expect(!v.is_empty());
    try testing.expect(!v.is_value());
    try testing.expect(v.is_layer());
    const info = v.as_layer();
    try testing.expectEqual(ptr, info.ptr);
    try testing.expect(info.is_leaf);
    const info2 = v.try_as_layer().?;
    try testing.expectEqual(ptr, info2.ptr);
}

test "LeafValue: try_as_value_ptr" {
    const LV = LeafValue(u64);
    var v = LV.init_value(10);
    const ptr = v.try_as_value_ptr().?;
    ptr.* = 20;
    try testing.expectEqual(@as(u64, 20), v.as_value());
}

test "SplitPoint: init" {
    const sp = SplitPoint{ .pos = 7, .split_ikey = 0xABCD };
    try testing.expectEqual(@as(usize, 7), sp.pos);
    try testing.expectEqual(@as(u64, 0xABCD), sp.split_ikey);
}

test "InsertTarget: values" {
    const t: InsertTarget = .left;
    try testing.expect(t == .left);
    try testing.expect(t != .right);
}
