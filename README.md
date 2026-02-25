# Masstree-Zig

A Zig implementation of the **Masstree** data structure — a high-performance,
cache-efficient, trie-of-B⁺-trees for variable-length byte-string keys.

## Overview

Masstree was introduced by Mao, Kohler, and Morris (EuroSys 2012).  It
combines:

- A **trie** that routes on 8-byte key slices for cache-line-friendly
  integer comparisons.
- **B⁺ trees** at each trie layer for sorted storage and range scans.
- (In the original paper) optimistic concurrency control.  This
  implementation provides the single-threaded algorithmic core.

## Project Structure

```
masstreez/
├── build.zig              Build system (library, test, bench targets)
├── build.zig.zon          Package manifest
├── README.md
├── docs/
│   ├── ARCHITECTURE.md    Module design, memory model, dependency graph
│   └── ALGORITHMS.md      Pseudocode, complexity analysis
├── src/
│   ├── root.zig           Public re-export module
│   ├── config.zig         FANOUT = 15, KEY_SLICE_LEN = 8
│   ├── key.zig            Key slicing: makeSlice, suffix, sliceCount
│   ├── leaf.zig           LeafNode (Entry, ValueOrLink, findPos, …)
│   ├── interior.zig       InteriorNode (findChildIdx, insertAt, …)
│   ├── layer.zig          Layer — one trie-level B⁺ tree
│   └── tree.zig           Masstree — top-level API
├── tests/
│   ├── key_tests.zig      Key slicing edge cases & ordering
│   ├── leaf_tests.zig     Leaf insert/remove/capacity
│   ├── interior_tests.zig Interior routing & shifting
│   ├── layer_tests.zig    Splits, sublayers, iteration
│   ├── tree_tests.zig     Full API: CRUD, 1000-key, prefix/binary keys
│   └── integration_tests.zig  500-key stress, deep trie, overwrites
└── bench/
    └── main.zig           8 benchmarks × 3 sizes (1K / 10K / 100K)
```

## Quick Start

```bash
# Run all tests (inline + extended + integration)
zig build test

# Run benchmarks (ReleaseFast)
zig build bench

# Build static library
zig build
```

## API

```zig
const Masstree = @import("src/root.zig").Masstree;

var tree = try Masstree.init(allocator);
defer tree.deinit();

try tree.put("hello", 42);
const val = tree.get("hello");   // ?usize → 42
_ = tree.remove("hello");        // true
tree.len();                       // 0
tree.isEmpty();                   // true
```

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — module decomposition,
  memory ownership, circular-import strategy.
- [docs/ALGORITHMS.md](docs/ALGORITHMS.md) — pseudocode for get / put /
  remove, leaf & interior splitting, complexity tables.

## References

- Y. Mao, E. Kohler, R. T. Morris.  *"Cache Craftiness for Fast
  Multicore Key-Value Storage."*  EuroSys 2012.
