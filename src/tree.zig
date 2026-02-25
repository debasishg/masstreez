//! Top-level **Masstree** API.
//!
//! A `Masstree` is a trie of B⁺ trees for variable-length byte-string
//! keys.  This module is the only entry point that library consumers
//! normally need.
//!
//! ## Thread safety
//!
//! This implementation is **single-threaded**.  The original Masstree
//! paper adds per-node version counters and optimistic concurrency
//! control for high-throughput concurrent access; that extension is a
//! natural next step on top of this algorithmic core.
//!
//! ## Example
//!
//! ```zig
//! var tree = try Masstree.init(allocator);
//! defer tree.deinit();
//!
//! try tree.put("hello", 42);
//! std.debug.assert(tree.get("hello").? == 42);
//! _ = tree.remove("hello");
//! std.debug.assert(tree.get("hello") == null);
//! ```

const std = @import("std");
const Allocator = std.mem.Allocator;
const Layer = @import("layer.zig").Layer;

pub const Masstree = struct {
    root_layer: *Layer,
    allocator: Allocator,
    count: usize = 0,

    /// Create a new, empty tree backed by `allocator`.
    pub fn init(allocator: Allocator) !Masstree {
        return .{
            .root_layer = try Layer.create(allocator, 0),
            .allocator = allocator,
        };
    }

    /// Free **all** memory owned by the tree (nodes, key copies, child
    /// layers).
    pub fn deinit(self: *Masstree) void {
        self.root_layer.deinit();
    }

    /// Insert or update a key-value pair.
    ///
    /// * If `key` already exists its value is overwritten and the entry
    ///   count does not change.
    /// * Otherwise a new entry is created and the count increments.
    pub fn put(self: *Masstree, key: []const u8, value: usize) !void {
        const existed = self.root_layer.get(key) != null;
        try self.root_layer.put(key, value);
        if (!existed) self.count += 1;
    }

    /// Look up `key`.  Returns `null` when the key is absent.
    pub fn get(self: *const Masstree, key: []const u8) ?usize {
        return self.root_layer.get(key);
    }

    /// Remove `key`.  Returns `true` if the key existed.
    pub fn remove(self: *Masstree, key: []const u8) bool {
        const removed = self.root_layer.remove(key);
        if (removed) self.count -= 1;
        return removed;
    }

    /// Current number of stored entries.
    pub fn len(self: *const Masstree) usize {
        return self.count;
    }

    /// `true` when the tree holds zero entries.
    pub fn isEmpty(self: *const Masstree) bool {
        return self.count == 0;
    }
};

// ─── Inline unit tests ──────────────────────────────────────────────────────

const testing = std.testing;

test "Masstree: basic CRUD" {
    var t = try Masstree.init(testing.allocator);
    defer t.deinit();

    try t.put("a", 1);
    try testing.expectEqual(@as(?usize, 1), t.get("a"));
    try testing.expectEqual(@as(usize, 1), t.len());

    try t.put("a", 2);
    try testing.expectEqual(@as(?usize, 2), t.get("a"));
    try testing.expectEqual(@as(usize, 1), t.len());

    try testing.expect(t.remove("a"));
    try testing.expectEqual(@as(?usize, null), t.get("a"));
    try testing.expectEqual(@as(usize, 0), t.len());
    try testing.expect(t.isEmpty());
}
