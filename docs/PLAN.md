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

## Phase 4 — Concurrency (~1,200 lines) ✅

Committed as `e720dea`. 114/114 tests passing (106 single-threaded + 8 concurrent).

Convert all data structures to concurrent versions with lock-free readers
and per-node locking for writers.

| Component | Description | Status |
|-----------|-------------|--------|
| `NodeVersion` atomics | CAS spinlock, `stable()`/`has_changed()`/`has_split()` OCC primitives | ✅ |
| `LockGuard` | `mark_insert()`/`mark_split()`/`release()` with version counter bumping | ✅ |
| Atomic leaf accessors | `load/store_permutation()` (u64), `load/store_next/prev/parent()` | ✅ |
| Atomic internode accessors | `load/store_nkeys()`, `load/store_child()`, `load/store_parent()` | ✅ |
| Tagged pointers | `config.tag_ptr/untag_ptr` with `usize root_tagged` | ✅ |
| Layer pointers | `value.init_layer/try_as_layer` for sublayer roots | ✅ |
| OCC read protocol (`get`) | `navigate_to_leaf_occ` (atomic nkeys + `load_child`) + B-link forward on `not_found` | ✅ |
| Write locking (`put`) | OCC nav → B-link boundary check → lock → insert/split → `propagate_split` | ✅ |
| Write locking (`remove`) | Same OCC nav + B-link boundary pattern as `put` | ✅ |
| Range iterators | OCC on internode nav helpers, `stable`+`has_changed` retry on leaf reads | ✅ |
| Multi-threaded tests | 8 scenarios: disjoint/overlapping/read-write/split-stress/interleaved | ✅ |

### Key design decisions for Phase 4:
- **OCC + B-link forward**: readers navigate internodes with optimistic version
  checks (`stable` → read → `has_changed`); on `not_found`, walk the B-link
  next-pointer chain comparing boundaries before returning null. This handles
  concurrent splits that complete between internode navigation and leaf access.
- **Boundary check instead of version-based split detection** for writers:
  after locking a leaf, compare key against the first key of `load_next()`.
  If key ≥ boundary, release lock and advance. More robust than `has_split()`
  checks which can miss splits that complete before the version snapshot.
- **Root reload on retry**: `put_at_layer` and `remove_at_layer` reload
  `root_tagged` on each OCC retry iteration for the main root case, since a
  concurrent split may have installed a new root above the stale pointer.
- **Atomic nkeys in OCC navigation**: `navigate_to_leaf_occ` uses `load_nkeys()`
  and `load_child()` (both atomic) instead of direct field reads, preventing
  torn reads on partially-updated internodes.
- **SPLIT_UNLOCK_MASK = LOCK_BIT | SPLITTING_BIT | INSERTING_BIT (= 7)**:
  `mark_insert()` is called before `mark_split()`, so `release()` must clear
  all three bits. Original value of 5 (missing INSERTING_BIT) caused `stable()`
  to spin forever on the leftover dirty bit.
- **ROOT_BIT sync after `propagate_split`**: `mark_node_nonroot` uses
  `@atomicRmw` directly on the version word, but `guard.locked_value` isn't
  updated. Fixed by syncing `locked_value`'s ROOT_BIT from the actual atomic
  value before `release()`.

### Bugs found and fixed:
1. `SPLIT_UNLOCK_MASK` missing `INSERTING_BIT` → `stable()` infinite spin
2. `ROOT_BIT` clobber in `create_new_root` → nonroot leaf restored to root
3. `stable()` hiding splits → use raw `@atomicLoad` for pre-lock snapshots
4. Non-OCC internode navigation for writers → switched to `navigate_to_leaf_occ`
5. Stale root pointer on retry → reload `root_tagged` each iteration
6. `get()` returning null without B-link check → added forward walk on `not_found`

## Phase 5 — Epoch-Based Reclamation (~1,500 lines) ✅

127/127 tests passing (13 new Phase 5 tests + 114 existing).

| Module | Rust Source | Description | Status |
|--------|------------|-------------|--------|
| `ebr.zig` | `seize` crate | Three-epoch collector with per-thread pins and retirement lists | ✅ |
| `node_pool.zig` | `pool.rs` | Thread-local size-class node pools for leaf/internode alloc | ✅ |
| `coalesce.zig` | `tree/coalesce.rs` | Lock-free Treiber stack queue for deferred empty-leaf cleanup | ✅ |
| Iterative teardown | `tree.rs` drop | Stack-based traversal replacing recursive `deinit()` | ✅ |
| EBR integration | — | Pin/unpin guards in `get`/`put`/`remove`, defer-retire in coalesce | ✅ |

### Key design decisions for Phase 5:
- **Three-epoch scheme**: global epoch counter (u64, monotonic). Items retired
  during epoch E are safe to reclaim at epoch E+2. Per-thread bins indexed by
  `epoch % 3`. `BATCH_THRESHOLD = 128` triggers epoch advancement attempt.
- **Collector as heap pointer**: `collector: *ebr.Collector` stored in `MassTree`
  so that `get(*const Self)` can call mutable collector methods through the
  pointer without violating Zig's const semantics.
- **Thread-local EBR state caching**: `threadlocal var tls_collector_addr: usize`
  keyed by collector pointer address enables per-collector thread registration
  without mutex overhead on the hot path.
- **Node pool size classes**: `CACHE_LINE = 64` granularity, up to 20 classes
  (1280 bytes max), capacity 512 per class per thread. Intrusive freelists
  reuse the node memory itself for the `next` pointer.
- **Lock-free coalesce queue**: Treiber stack (atomic CAS on head pointer).
  `MAX_REQUEUE = 10` attempts before dropping entries. `process_batch(limit)`
  locks leaf → verifies empty → marks deleted → unlinks B-link → removes from
  parent internode → retires via EBR guard.
- **Iterative teardown**: `destroy_tree_iterative()` uses `ArrayList(TraversalWork)`
  stack with tagged union variants (`.visit`, `.free_leaf`, `.free_internode`).
  LIFO ordering ensures children are freed before parents. Replaces recursive
  `destroy_node()` to avoid stack overflow on deep trees.

### Bugs found and fixed:
1. Zig 0.15 `ArrayList` API change: `init()` no longer takes allocator; allocator
   passed to individual methods (`deinit(allocator)`, `append(allocator, item)`)
2. `popOrNull()` renamed to `pop()` (returns `?T`) in Zig 0.15

## Phase 6 — Performance & Polish (~500 lines) ✅

140/140 tests passing (13 new Phase 6 tests + 127 existing).

| Module | Description | Status |
|--------|-------------|--------|
| `prefetch.zig` | `@prefetch` wrappers: internode CL1, child, grandchild, leaf read/write, blink-ahead | ✅ |
| `shard_counter.zig` | 16×128B-aligned sharded counter, FNV-1a thread mapping, `threadlocal` caching | ✅ |
| Branch hints | `@branchHint(.unlikely)` on OCC retry, B-link forward, error paths in tree.zig | ✅ |
| tree.zig integration | Prefetch in `navigate_to_leaf_occ`/`get`/`put`/`remove`; `ShardedCounter` replaces `count` | ✅ |
| Benchmarks | `bench/main.zig` rewritten: 9 single-threaded + 4 concurrent benchmarks | ✅ |
| build.zig | Bench step enabled with `createModule` + `addImport` | ✅ |
| Documentation | ARCHITECTURE.md and ALGORITHMS.md fully rewritten for concurrent architecture | ✅ |

### Key design decisions for Phase 6:
- **Prefetch comptime gate**: `enable_prefetch = true` allows A/B benchmarking
  by flipping to `false`. All prefetch functions become no-ops at comptime.
- **Pointer casting for `@prefetch`**: Zig's `@prefetch` requires `[*]const u8`.
  Wrapper functions accept `?*const anyopaque` and use `@ptrCast` for ergonomic
  call sites. Null pointers are silently ignored (no-op).
- **Sharded counter shard count = 16**: Matches Rust (`NUM_SHARDS = 16`), each
  shard is `align(128)` to avoid false sharing on platforms with 128-byte cache
  lines (Apple M-series). Thread-to-shard uses FNV-1a hash with `threadlocal`
  caching and `@branchHint(.cold)` on the compute path.
- **`load()` clamps to 0**: Sum of shards can go briefly negative under extreme
  concurrent decrement; `@max(sum, 0)` prevents underflow.
- **Concurrent benchmarks**: 4 scenarios (disjoint insert, read-only, mixed ops,
  hot-key contention) × 4 thread counts (1, 2, 4, 8). Workers use 8-byte
  big-endian keys for minimal overhead.

## Estimated Size

| Phase | Zig Lines (est.) | Actual |
|-------|-----------------|--------|
| Phase 1 | ~3,000 | ~3,200 |
| Phase 2 | ~2,500 | ~1,960 |
| Phase 3 | ~2,000 | ~775 |
| Phase 4 | ~2,500 | ~1,200 |
| Phase 5 | ~1,500 | ~957 (new) + ~150 (integration) |
| Phase 6 | ~500 | ~459 (src) + ~388 (bench) |
| **Total** | **~12,000** | **~8,223 src + 782 tests + 388 bench** |

Current codebase: 8,223 lines in `src/`, 782 lines in `tests/`, 388 lines in
`bench/`, 140/140 tests passing.

(vs ~25,000 lines Rust — Zig is more concise due to comptime generics, no
trait boilerplate, no unsafe ceremony)
