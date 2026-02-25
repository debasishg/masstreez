//! Public re-export module for the **masstree-zig** library.
//!
//! Consumers of the library only need:
//!
//! ```zig
//! const masstree = @import("root.zig");   // or via build.zig module name
//! var tree = try masstree.Masstree.init(allocator);
//! ```
//!
//! All internal types are also re-exported for advanced use cases
//! (custom iteration, node inspection, etc.).

pub const Masstree = @import("tree.zig").Masstree;
pub const Layer = @import("layer.zig").Layer;
pub const LeafNode = @import("leaf.zig").LeafNode;
pub const Entry = @import("leaf.zig").Entry;
pub const ValueOrLink = @import("leaf.zig").ValueOrLink;
pub const InteriorNode = @import("interior.zig").InteriorNode;
pub const ChildPtr = @import("interior.zig").ChildPtr;
pub const key = @import("key.zig");
pub const config = @import("config.zig");

test {
    // Pull in tests from every module so `zig build test` on root.zig
    // runs the full inline-test suite.
    const t = @import("std").testing;
    t.refAllDeclsRecursive(@This());
}
