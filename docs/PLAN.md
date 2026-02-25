# Masstree Zig Port — Implementation Plan

## Overview

Full concurrent port of the Rust masstree implementation (`../masstree/`) to Zig.
The Rust codebase (~25K lines) is a lock-free concurrent trie of B⁺ trees with
OCC reads, per-node spin locks for writes, epoch-based reclamation, and a 64-bit
permutation word for slot ordering.

**Decision: Rewrite, not incremental enhancement.** The Rust codebase's SoA leaf
layout, permuter, atomic fields, and EBR are fundamentally incompatible with the
v0.1 Zig code's AoS layout, physical array shifting, and immediate-free memory.

The previous single-threaded implementation is preserved on branch
`v0.1-single-threaded`.

## Phase 0 — Branch & Scaffold ✅
- [x] Create `v0.1-single-threaded` branch from current HEAD
- [x] Push branch to remote
- [x] Replace `src/*.zig` with new module skeleton
- [x] Update `root.zig` re-exports

## Phase 1 — Core Data Structures (~3,000 lines) ✅

Port the foundational structs. All fields use non-atomic types initially;
Phase 4 converts them to atomics. The **layout** matches the Rust concurrent
design so atomics can be dropped in without restructuring.

Committed as `f8e1c44`. 64/64 tests passing.

| Module | Rust Source | Rust Lines | Description | Status |
|--------|------------|------------|-------------|--------|
| `key.zig` | `key.rs` | 431 | `Key` struct with `ikey`, `shift()`, suffix tracking | ✅ |
| `permuter.zig` | `permuter.rs` | 938 | `Permuter(15)` — 64-bit encoded slot permutation | ✅ |
| `node_version.zig` | `nodeversion.rs` | 1,271 | OCC version word with lock, dirty flags, counters | ✅ |
| `suffix.zig` | `suffix.rs` | 600 | `SuffixBag` — contiguous per-slot suffix storage | ✅ |
| `value.zig` | `value.rs` | 198 | `LeafValue(V)` — tagged union: empty/value/layer | ✅ |
| `leaf.zig` | `leaf15.rs` | 2,510 | `LeafNode(V)` — SoA layout with permuter | ✅ |
| `internode.zig` | `internode.rs` | 1,050 | `InternodeNode` — routing with version word | ✅ |
| `config.zig` | — | — | Constants: `FANOUT=15`, `KEY_SLICE_LEN=8` | ✅ |

### Key design decisions for Phase 1:
- `Permuter(WIDTH)` uses comptime generic `WIDTH` (default 15)
- `LeafNode` uses SoA: separate `ikey[15]`, `keylenx[15]`, `values[15]` arrays
- `keylenx` discriminator: 0–8 = inline key length, 64 = has suffix, ≥128 = layer
- No physical array shifting — permuter handles logical ordering
- `NodeVersion` is a `u32` bitfield matching C++/Rust layout exactly
- Structs get `align(64)` for cache-line alignment from the start

### Lessons learned:
- Zig regular structs do NOT guarantee field ordering — the compiler reorders
  fields for alignment optimization. Cannot cast `*anyopaque` → `*NodeVersion`
  to call `is_leaf()` since `version` is NOT at offset 0.

## Phase 2 — Tree Operations, Single-Threaded (~2,500 lines) ✅

Port core algorithms using Phase 1 structs but without atomics or guards.

Committed as `6c44e7e`. 80/80 tests passing (16 tree tests + 64 Phase 1 tests).

| Module | Rust Source | Description | Status |
|--------|------------|-------------|--------|
| `tree.zig` | `tree.rs`, `tree/generic/` | `MassTree(V)` — public API with layer management | ✅ |

### Operations:
- [x] `get(key)` — descend via internode, scan leaf via permuter, check suffix
- [x] `put(key, value)` — find leaf, insert via permuter, split on full, sublayer on collision
- [x] `remove(key)` — find slot, remove from permuter, return old value
- [x] `split_leaf()` — allocate new leaf, divide via permuter, propagate separator

### Key design decisions for Phase 2:
- `root_is_leaf: bool` in `MassTree` struct + `internode.height == 0` for node
  type detection (avoids `*anyopaque` → `*NodeVersion` cast; see Phase 1 lessons)
- Layer roots always start as leaves
- Twig chain algorithm for keys sharing 8-byte prefixes
- Recursive destruction handles sublayers

## Phase 3 — Range Scan (~2,000 lines) ✅

Committed as part of the Phase 3 work. 102/102 tests passing (17 range tests).

| Module | Rust Source | Description | Status |
|--------|------------|-------------|--------|
| `range.zig` | `tree/range/` | `CursorKey`, `RangeBound`, `RangeIterator(V)`, `ReverseRangeIterator(V)` | ✅ |

- [x] `CursorKey` — stack-based key prefix accumulator with `shift()`/`unshift()`/`build_key()`
- [x] `RangeBound` — included/excluded/unbounded with `contains_start()`/`contains_end()`
- [x] `RangeIterator(V)` — forward iteration with layer stack, start/end bound checking
- [x] `ReverseRangeIterator(V)` — backward iteration via `prev` pointers and reversed layer descent
- [x] Integration: `MassTree.range()`, `range_reverse()`, `range_all()` convenience methods

### Key design decisions for Phase 3:
- No state machine needed for single-threaded: iterators use direct loop with
  layer stack push/pop. OCC Retry state will be added in Phase 4.
- `start_active` flag: after navigating to approximate position via `lower_bound`,
  entries before start bound are skipped until first match. Avoids complex
  exact-position initialization while keeping overhead to O(1) extra checks.
- Key reconstruction via `CursorKey.build_key()`: prefix from ancestor layers
  + current ikey + suffix → full key in iterator's internal buffer.
- Entry keys are slices into the iterator's internal buffer, valid until next `next()` call.
- Layer descent: forward goes to leftmost leaf, reverse goes to rightmost leaf.

## Phase 4 — Concurrency (~2,500 lines)

Convert all data structures to concurrent versions.

| Component | Description |
|-----------|-------------|
| Atomic fields | `@atomicLoad`/`@atomicStore` on ikey, keylenx, values, pointers |
| `AtomicPermuter` | CAS-based permuter (linearization point for inserts) |
| `NodeVersion` atomics | `AtomicU32` with backoff spin-lock |
| OCC read protocol | `stable()` → read → `has_changed()` validation |
| Write locking | Lock → validate → mutate → unlock with hand-over-hand splits |
| Guard-based API | All public methods take `*Guard` parameter |

## Phase 5 — Epoch-Based Reclamation (~1,500 lines)

| Module | Description |
|--------|-------------|
| `ebr.zig` | Three-epoch collector with per-thread pins and retirement lists |
| `node_pool.zig` | Thread-local size-class node pools for leaf/internode alloc |
| Coalesce queue | Lock-free queue of empty leaves for deferred cleanup |
| Iterative teardown | Stack-based traversal replacing recursive `deinit()` |

## Phase 6 — Performance & Polish (~500 lines)

| Component | Description |
|-----------|-------------|
| `prefetch.zig` | `@prefetch` for next-node prefetching during descent |
| `shard_counter.zig` | `align(64)` per-thread counters for approximate `len()` |
| Branch hints | `@branchHint(.unlikely)` on error/retry paths |
| Benchmarks | Update `bench/main.zig` for generic API + concurrent benchmarks |
| Documentation | Update ARCHITECTURE.md and ALGORITHMS.md |

## Estimated Size

| Phase | Zig Lines (est.) |
|-------|-----------------|
| Phase 1 | ~3,000 |
| Phase 2 | ~2,500 |
| Phase 3 | ~2,000 |
| Phase 4 | ~2,500 |
| Phase 5 | ~1,500 |
| Phase 6 | ~500 |
| **Total** | **~12,000** |

(vs ~25,000 lines Rust — Zig is more concise due to comptime generics, no
trait boilerplate, no unsafe ceremony)
