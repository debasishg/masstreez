# Architecture

## High-Level Design

Masstree is a **trie of B+ trees** designed for high-performance
concurrent in-memory key-value storage.  Variable-length byte-string keys
are partitioned into 8-byte slices.  Each trie layer handles one 8-byte
slice and stores it in a B+ tree.  When two keys share the same 8-byte
prefix at a given trie depth, a **child trie layer** is created to
distinguish them using the next 8 bytes.

```
Key: "abcdefghijklmnop" (16 bytes)
      +-- Layer 0 B+ tree key: u64 from "abcdefgh"
      +-- Layer 1 B+ tree key: u64 from "ijklmnop"
```

This design gives Masstree two fundamental advantages:

1. **Cache efficiency** -- comparisons within a B+ tree node are 8-byte
   integer comparisons (u64), not byte-by-byte memcmp.
2. **Adaptive depth** -- short keys resolve at shallow depths; long keys
   only pay for the extra layers they actually need.

## Concurrency Model

The implementation is fully concurrent, supporting lock-free reads and
fine-grained locked writes:

- **Reads** use **Optimistic Concurrency Control (OCC)** -- read the node
  version, perform the operation, re-check the version.  No locks held.
- **Writes** acquire a per-node **CAS spinlock** embedded in the version
  word.  Lock scope is minimised: only the target leaf (and parent on
  split) are locked.
- **B-link tree** -- every leaf has a `next` pointer.  If a concurrent
  split moves the target key to a new sibling, readers follow the
  B-link forward chain without retrying from the root.
- **Epoch-Based Reclamation (EBR)** -- deleted nodes are deferred until
  all readers that might hold a reference have completed.

## Module Decomposition

```
src/
  config.zig          Compile-time constants, tagged pointer utilities
  key.zig             Key representation, ikey extraction, trie shifting
  permuter.zig        Permuter15 -- u64-packed logical-to-physical slot map
  suffix.zig          SuffixBag -- overflow key storage for keys > 8 bytes
  value.zig           LeafValue(V) -- tagged union (value | empty | layer)
  node_version.zig    NodeVersion -- OCC version word with embedded spinlock
  leaf.zig            LeafNode(V) -- B+ tree leaf, SoA layout, B-link
  interior.zig        InternodeNode -- B+ tree branch with routing keys
  layer.zig           Layer operations within one trie depth
  ebr.zig             Epoch-Based Reclamation (3-epoch, per-thread)
  node_pool.zig       Thread-local size-class node allocator
  coalesce.zig        Lock-free coalesce queue for deferred cleanup
  prefetch.zig        @prefetch wrappers for cache-line prefetching
  shard_counter.zig   ShardedCounter -- 16-shard contention-free counter
  range.zig           Range iteration with OCC + layer-stack descent
  tree.zig            MassTree(V) -- public API, orchestrates everything
  root.zig            Library re-export module
```

### Dependency Graph

```
root.zig (re-exports all modules)
  |
  +-> tree.zig
        +-- leaf.zig <-- permuter.zig
        |     +-- node_version.zig
        |     +-- suffix.zig
        |     +-- value.zig <-- config.zig
        |     +-- key.zig <-- config.zig
        +-- interior.zig <-- node_version.zig
        +-- range.zig <-- (leaf, interior, key,
        |                   node_version, ebr,
        |                   config, prefetch)
        +-- layer.zig <-- (config, key, leaf, interior)
        +-- ebr.zig
        +-- node_pool.zig
        +-- coalesce.zig <-- (leaf, interior, ebr, node_pool)
        +-- prefetch.zig
        +-- shard_counter.zig
        +-- config.zig
```

## Key Components

### `config.zig` -- Tagged Pointers & Constants

| Constant        | Value | Purpose                                     |
|-----------------|-------|---------------------------------------------|
| `FANOUT`        | 15    | Max keys per B+ tree node (leaf or interior) |
| `KEY_SLICE_LEN` | 8     | Bytes consumed per trie layer               |

Provides `tag_ptr` / `untag_ptr` which pack a type bit into the low bit
of aligned pointers, enabling lock-free discrimination between leaf and
interior nodes in child-pointer arrays.

### `key.zig` -- Key Representation

The `Key` struct caches the **ikey** (u64 from the current 8-byte
slice), a `shift_count` tracking trie depth, and the raw `data` slice.
Big-endian encoding ensures numeric comparison matches lexicographic
order.

### `node_version.zig` -- OCC Version Word

A 32-bit word packing all concurrency metadata:

| Bits     | Field      | Purpose                                      |
|----------|-----------|----------------------------------------------|
| 0        | LOCK      | CAS spinlock for exclusive write access       |
| 1        | INSERTING | Dirty flag: insertion in progress              |
| 2        | SPLITTING | Dirty flag: split in progress                  |
| 6-15     | VINSERT   | 10-bit insert version counter                 |
| 16-27    | VSPLIT    | 12-bit split version counter                  |
| 28       | DELETED   | Tombstone: node logically removed             |
| 29       | ROOT      | This node is a B+ tree root                   |
| 30       | ISLEAF    | This node is a leaf (vs interior)              |

**OCC protocol:**
1. `stable()` -- spin until LOCK, INSERTING, SPLITTING are all clear.
2. Read the node's data.
3. `has_changed(v_before)` -- compare current version to snapshot; retry
   if they differ.

**Lock protocol:** `lock()` uses `@cmpxchgWeak(.acquire, .monotonic)`.
`release()` bumps the appropriate version counter and clears LOCK +
dirty flags with `@atomicStore(.release)`.

### `permuter.zig` -- Permutation Word

A single u64 encodes both the count (4 bits) and a permutation of
15 x 4-bit physical slot indices.  This allows **lock-free insert
linearisation**: the new entry is written to a free physical slot, then
the permuter word is atomically stored with the new logical ordering.
Readers scanning during an insert see either the old or new permuter --
both are consistent snapshots.

### `leaf.zig` -- Leaf Node (SoA Layout)

Struct-of-Arrays design for cache efficiency:

- `ikeys[15]` -- 8-byte integer keys (120 bytes, fits in 2 cache lines)
- `keylenx[15]` -- suffix/layer discriminator flags
- `values[15]` -- LeafValue(V) slots
- `suffix: SuffixBag` -- overflow storage for long keys
- `permutation: Permuter15` -- logical ordering
- `version: NodeVersion` -- OCC + lock
- `next` / `prev` -- B-link doubly-linked list
- `parent` -- back-pointer to enclosing internode

### `interior.zig` -- Interior (Internode)

Stores up to 15 sorted routing keys and 16 child pointers (tagged to
discriminate leaf vs interior).  Same NodeVersion OCC protocol as leaves.

### `ebr.zig` -- Epoch-Based Reclamation

Three-epoch scheme (current, current-1, current-2):

1. **`pin()`** -- register thread in current epoch.
2. Read/write operations proceed.
3. **`unpin()`** -- mark thread as inactive.
4. Items `defer_retire()`d in epoch E become reclaimable once all threads
   that were pinned in epoch E have unpinned and the global epoch has
   advanced past E+1.

The global epoch advances when every pinned thread has observed it.
Per-thread retire lists are drained lazily during `pin()`.

### `node_pool.zig` -- Thread-Local Node Pool

Bucketed by size class (1-20 cache lines).  Each thread maintains
threadlocal intrusive freelists avoiding all synchronisation.  Free
nodes are recycled within the same thread; cross-thread recycling occurs
naturally through EBR (retired nodes are freed centrally, then re-used
by whichever thread allocates next).

### `coalesce.zig` -- Deferred Cleanup Queue

A **Treiber stack** (lock-free LIFO via atomic CAS on a head pointer).
Empty leaves are enqueued here rather than immediately cleaned up.
A background sweep:
1. Pops an entry.
2. Locks the leaf, verifies it's still empty.
3. Marks it DELETED, unlinks from B-link chain.
4. Removes entry from parent internode.
5. Retires via EBR.

Re-queues entries (up to MAX_REQUEUE = 10) if the leaf is no longer
empty or the lock is contended.

### `prefetch.zig` -- Cache-Line Prefetch Hints

Wraps Zig's `@prefetch` builtin with domain-specific helpers:

| Function                    | Strategy                                       |
|----------------------------|-------------------------------------------------|
| `prefetch_internode_keys`   | CL1 (offset +64) of ikeys before key search    |
| `prefetch_child`            | Selected child pointer after routing            |
| `prefetch_grandchild`       | Speculative prefetch of grandchild during OCC   |
| `prefetch_leaf_read`        | Two cache lines of leaf data before scan        |
| `prefetch_leaf_write`       | Write-intent prefetch before locking            |
| `prefetch_blink_ahead`      | Next-next leaf in B-link chain (2-hop ahead)    |

Controlled by comptime `enable_prefetch` flag for benchmarking A/B tests.

### `shard_counter.zig` -- Sharded Approximate Counter

16 x 128-byte-aligned shards, each containing an atomic.Value(isize).
Thread-to-shard mapping via FNV-1a hash of Thread.getCurrentId(),
cached in threadlocal. Avoids false sharing on `len()` hot path.

- `increment()` / `decrement()` -- monotonic atomic add on shard.
- `load()` -- sums all 16 shards (suitable for approximate length).

### `range.zig` -- Range Iteration

Forward and reverse iteration across the trie.  Uses a **layer stack**
to track descent through trie layers.  CursorKey accumulates prefix
bytes across layers to reconstruct full keys during iteration.

OCC is used throughout: each internode/leaf access is bracketed by
`stable()` / `has_changed()` checks, with automatic retry on version
mismatch.

### `tree.zig` -- MassTree(V) Public API

The top-level generic struct:

```zig
pub fn MassTree(V: type) type {
    return struct {
        root_tagged: usize,           // tagged pointer (low bit = is_leaf)
        shard_count: ShardedCounter,   // approximate len()
        allocator: Allocator,
        collector: *Collector,         // EBR
        coalesce_queue: *CoalesceQueue(V),
        // ...
    };
}
```

**Public API:**
- `init(allocator) -> !Self` / `deinit()`
- `get(key) -> ?V` -- lock-free OCC read
- `put(key, value) -> !void` -- locked insert/update with auto-split
- `remove(key) -> ?V` -- locked delete with coalesce queuing
- `len() -> usize` / `is_empty() -> bool`
- `range_all()` / `range_from(start)` / `range_from_to(start, end)` -- iterators

**Internal flow (read path):**
1. Pin EBR epoch.
2. Navigate to leaf via OCC (prefetching internode keys, child, grandchild).
3. Scan leaf with permuter-aware key matching.
4. Follow B-link if key might be in next sibling (with blink-ahead prefetch).
5. Verify OCC version; retry from navigate if changed.
6. Recurse into sublayer if keylenx == LAYER.
7. Unpin EBR epoch.

**Internal flow (write path):**
1. Pin EBR epoch.
2. OCC navigate to leaf.
3. Prefetch leaf with write intent.
4. Lock leaf (version.lock()).
5. Verify key still belongs here (or follow B-link under lock).
6. Insert/update/delete within locked leaf.
7. If leaf is full: split -> lock parent -> insert separator -> possibly
   cascade splits up the tree.
8. Release lock(s), unpin EBR.

## Memory Management

All nodes are allocated through a caller-provided `std.mem.Allocator`,
optionally recycled through thread-local node_pool. Deallocation is
managed by the EBR collector -- nodes and key copies are retired (not
immediately freed) and reclaimed once all concurrent readers have
completed.

`deinit()` performs an **iterative** (not recursive) teardown of the
entire trie, using an explicit stack to avoid stack overflow on deep
tries.

## Concurrency Primitives Summary

| Primitive                  | Module           | Purpose                           |
|---------------------------|-----------------|-----------------------------------|
| CAS spinlock              | node_version     | Per-node write exclusion          |
| Atomic load (acquire)     | node_version     | OCC read snapshot                 |
| Atomic store (release)    | node_version     | Version bump + unlock             |
| Atomic store (seq_cst)    | node_version     | Dirty flag publication            |
| Atomic CAS                | coalesce         | Treiber stack push/pop            |
| Atomic Value(u64)         | ebr              | Global epoch counter              |
| Atomic Value(isize)       | shard_counter    | Per-shard increment/decrement     |
| threadlocal               | tree, node_pool, shard_counter | Per-thread state  |
| @prefetch                 | prefetch         | Latency hiding during traversal   |
| Tagged pointers           | config, value    | Lock-free type discrimination     |
