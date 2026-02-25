# Architecture

## High-Level Design

Masstree is a **trie of B⁺ trees**.  Variable-length byte-string keys are
partitioned into 8-byte slices.  Each trie layer handles one 8-byte slice
and stores it in a B⁺ tree.  When two keys share the same 8-byte prefix at
a given trie depth, a **child trie layer** is created to distinguish them
using the next 8 bytes.

```
Key: "abcdefghijklmnop" (16 bytes)
      ├─ Layer 0 B⁺ tree key: u64 from "abcdefgh"
      └─ Layer 1 B⁺ tree key: u64 from "ijklmnop"
```

This design gives Masstree two fundamental advantages:

1. **Cache efficiency** — comparisons within a B⁺ tree node are 8-byte
   integer comparisons (`u64`), not byte-by-byte `memcmp`.
2. **Adaptive depth** — short keys resolve at shallow depths; long keys
   only pay for the extra layers they actually need.

## Module Decomposition

```
src/
├── config.zig      Compile-time constants (FANOUT, KEY_SLICE_LEN)
├── key.zig         Pure functions: makeSlice, suffix, sliceCount
├── leaf.zig        LeafNode — B⁺ tree leaf with sorted entries
├── interior.zig    InteriorNode — B⁺ tree branch with separator keys
├── layer.zig       Layer — one complete B⁺ tree at a trie depth
├── tree.zig        Masstree — public API wrapping the root Layer
└── root.zig        Re-export module for library consumers
```

### Dependency graph

```
tree.zig ──→ layer.zig ──→ leaf.zig ──→ key.zig ──→ config.zig
                       └──→ interior.zig ──→ leaf.zig
root.zig ──→ (all of the above)
```

### `config.zig`

Compile-time constants:

| Constant        | Value | Purpose                                      |
|-----------------|-------|----------------------------------------------|
| `FANOUT`        | 15    | Max keys per B⁺ tree node (leaf or interior) |
| `KEY_SLICE_LEN` | 8     | Bytes consumed per trie layer                |

### `key.zig`

Pure, allocation-free functions for key manipulation:

- **`makeSlice(key, depth) → u64`** — extracts 8 bytes at a given trie
  depth, zero-pads short keys, encodes as big-endian `u64`.
- **`suffix(key, depth) → []const u8`** — returns the tail beyond the
  given depth.
- **`sliceCount(key) → usize`** — `⌈len / 8⌉`.

### `leaf.zig`

B⁺ tree leaf node.  Up to `FANOUT` entries, each containing:

| Field       | Type          | Description                              |
|-------------|---------------|------------------------------------------|
| `key_slice` | `u64`         | 8-byte trie key at this depth            |
| `full_key`  | `[]const u8`  | Owned copy of the complete key           |
| `val`       | `ValueOrLink` | `usize` payload **or** child-layer link  |
| `key_len`   | `usize`       | Original key length                      |

Leaves are linked in a **doubly-linked list** for range iteration.

### `interior.zig`

B⁺ tree interior (branch) node.  Up to `FANOUT` separator keys and
`FANOUT + 1` child pointers.  Children may be leaves or other interiors.

### `layer.zig`

One trie level — owns a B⁺ tree root and all its nodes.  Operations:

- `get(key) → ?usize`
- `put(key, value) → !void` (with recursive splitting)
- `remove(key) → bool`
- `iterator() → Iterator`

When a leaf entry already exists for a key slice but the full keys differ,
`put` transparently creates a child `Layer` at `depth + 1` and migrates
both entries.

### `tree.zig`

The public `Masstree` struct.  Wraps the root `Layer` at depth 0 and
maintains an entry count.

## Memory Management

All nodes and key copies are allocated through a caller-provided
`std.mem.Allocator`.  `deinit()` recursively frees:

1. Child trie layers (via `ValueOrLink.link`)
2. Interior nodes (depth-first)
3. Leaf nodes and their owned key copies

No garbage collection or reference counting is used.

## Breaking the Circular Import

`leaf.zig` defines `ValueOrLink` with `.link: *anyopaque` instead of
`*Layer`.  This avoids the `leaf ↔ layer` circular import.  The cast
from `*anyopaque` to `*Layer` happens exclusively in `layer.zig`.
