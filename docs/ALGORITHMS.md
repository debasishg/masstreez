# Algorithms

This document describes the core algorithms used in the concurrent
Masstree implementation, with pseudocode and complexity analysis.

---

## 1. Key Slicing

Given a variable-length byte key K and a trie depth d (0-indexed):

```
slice = K[d*8 .. (d+1)*8]     // zero-padded if key is short
u64   = big_endian(slice)
```

**Why big-endian?**  Big-endian encoding ensures that the numeric
comparison of u64 values matches the lexicographic comparison of
the original byte strings.

```
"apple___" -> 0x6170706C655F5F5F
"banana__" -> 0x62616E616E615F5F
0x6170... < 0x6261...  (matches "apple" < "banana")
```

**Complexity:** O(1) per slice extraction.

---

## 2. Optimistic Concurrency Control (OCC)

Every node (leaf and interior) contains a 32-bit **version word** with
an embedded spinlock, dirty flags, and version counters.

### 2a. Read Protocol

```
function OCC_READ(node):
    loop:
        v1 = node.version.stable()          // spin until unlocked & clean
        result = read_data(node)             // read without holding lock
        if node.version.has_changed(v1):
            continue                          // version changed -> retry
        return result
```

`stable()` spins while LOCK, INSERTING, or SPLITTING bits are set,
ensuring the reader sees a quiescent snapshot.  `has_changed()` compares
the full version word -- if any version counter has bumped, the data may
be inconsistent and the read is retried.

**Key property:** Readers never block writers.  Readers may retry, but
retries are bounded because writers complete quickly (sub-microsecond
critical sections).

### 2b. Write Protocol

```
function OCC_WRITE(node, update_fn):
    node.version.lock()                      // CAS spinlock
    node.version.mark_dirty()                // set INSERTING/SPLITTING
    update_fn(node)                          // modify node in-place
    node.version.release()                   // bump version + unlock (release fence)
```

The release fence ensures all node mutations are visible before the
version change.  Writers hold the lock for the minimum duration --
typically a single leaf node update.

---

## 3. B-Link Tree Forward Walk

When a concurrent split moves the target key range into a new right
sibling, readers follow the **B-link chain** instead of restarting from
the root.

```
function FORWARD_WALK(leaf, target_key):
    current = leaf
    while current.next != null:
        next = @atomicLoad(current.next)
        if next.first_key > target_key:
            break                            // target is in current
        current = next                       // follow B-link
    return current
```

This works because splits always move the upper half of keys to the
right sibling, preserving the invariant that next.first_key is the
boundary.

**Prefetch optimisation:** During forward walk, `prefetch_blink_ahead`
is called on next.next (2-hop lookahead) to hide memory latency
on the next iteration.

---

## 4. Lookup (get)

```
function GET(tree, key):
    guard = tree.collector.pin()             // EBR: protect against reclamation
    defer guard.unpin()

    RETRY:
        leaf, v_leaf = navigate_to_leaf_occ(tree.root, key)
        if leaf is null:  return null

        // Scan leaf (OCC -- no lock held)
        result = leaf.find_key(key, permuter)
        match result:
            exact_match(slot):
                value = leaf.values[slot]
                if leaf.version.has_changed(v_leaf): goto RETRY
                match value:
                    .value(v): return v
                    .layer(sublayer): key.shift(); continue at sublayer root
            not_found:
                // Key might have moved to next sibling via concurrent split
                next = leaf.next
                if leaf.version.has_changed(v_leaf): goto RETRY
                if next != null and should_forward(key, next):
                    current = next
                    // follow B-link chain (with blink-ahead prefetch)
                    goto RETRY with current
                return null
```

### navigate_to_leaf_occ

```
function NAVIGATE_TO_LEAF_OCC(root_tagged, key):
    current = untag(root_tagged)

    while current is interior:
        inode = current as interior
        prefetch_internode_keys(inode)        // CL1 of ikeys
        v = inode.version.stable()

        idx = binary_search(inode.ikeys, key.ikey)
        child = inode.children[idx]
        prefetch_child(child)                 // child's first cache line

        if inode.version.has_changed(v):
            @branchHint(.unlikely)
            continue from root                // OCC retry

        prefetch_grandchild(child, inode)     // speculative grandchild
        current = child

    return (current as leaf, version_snapshot)
```

**Complexity:** O(H) interior levels x O(log F) per level (binary search
over 15 keys), plus O(F) leaf scan.  Across L trie layers: O(L * H * F).

---

## 5. Insertion (put)

```
function PUT(tree, key, value):
    guard = tree.collector.pin()
    defer guard.unpin()

    LAYER_LOOP:
        leaf, v_leaf = navigate_to_leaf_occ(current_root, key)

        // OCC retry if navigation was stale
        if leaf is null: goto LAYER_LOOP

        prefetch_leaf_write(leaf)             // write-intent prefetch

        leaf.version.lock()                   // acquire per-node spinlock

        // Verify leaf is correct after locking (B-link check)
        if key belongs in leaf.next:
            leaf.version.release()
            leaf = leaf.next
            leaf.version.lock()

        search_result = leaf.locked_find(key)
        match search_result:
            found(slot):
                if keylenx[slot] == LAYER:
                    sublayer = values[slot].as_layer()
                    leaf.version.release()
                    key.shift()
                    current_root = sublayer
                    goto LAYER_LOOP           // descend into sublayer

                // Simple overwrite
                leaf.values[slot] = value
                leaf.version.release()

            not_found:
                if leaf is not full:
                    insert_into_leaf(leaf, key, value)
                    leaf.permutation = new_permuter   // linearisation point
                    leaf.version.release()
                    tree.shard_count.increment()
                else:
                    (split_key, new_leaf) = split_leaf(leaf, key, value)
                    propagate_split(leaf, new_leaf, split_key)
                    tree.shard_count.increment()

            conflict(slot):
                // Same ikey but different full key -> create sublayer
                sublayer = create_sublayer(leaf.entry[slot], key, value)
                leaf.keylenx[slot] = LAYER
                leaf.values[slot] = layer_ptr(sublayer)
                leaf.version.release()
                tree.shard_count.increment()
```

### 5a. Leaf Split

When a leaf is full and a new entry must be added:

1. Allocate a new leaf from node_pool.
2. Distribute entries: lower half stays, upper half moves to new leaf.
3. Set B-link: new_leaf.next = old_leaf.next; old_leaf.next = new_leaf
   (atomic store for old_leaf.next -- readers see atomically).
4. The **promoted separator** (split key) is propagated upward.

### 5b. Split Propagation

```
function PROPAGATE_SPLIT(leaf, new_leaf, split_key):
    parent = leaf.parent
    if parent is null:
        // Create new root
        new_root = alloc_interior(split_key, leaf, new_leaf)
        tree.root = tag(new_root)
        return

    parent.version.lock()
    if parent is full:
        // Interior split -- hand-over-hand locking
        (parent_split_key, new_interior) = split_interior(parent, split_key, new_leaf)
        parent.version.release()
        propagate_split(parent, new_interior, parent_split_key)  // cascade
    else:
        insert_child(parent, split_key, new_leaf)
        parent.version.release()
```

**Hand-over-hand locking:** During split cascades, the parent lock is
acquired while the child lock is still held, then the child lock is
released.  This prevents concurrent readers from observing an inconsistent
parent-to-child relationship.

### 5c. Sublayer Creation

When two keys collide on the same 8-byte ikey at depth d:

1. Create a new root leaf at depth d + 1.
2. Insert both the existing entry and the new entry into the sublayer.
3. Replace the leaf entry's value with a tagged layer pointer.
4. The keylenx byte is set to LAYER_KEYLENX (128) to mark the slot.

This recursion continues for as many 8-byte chunks as needed.

**Complexity:** O(F) per split.  Amortized O(1) per insert.

---

## 6. Deletion (remove)

```
function REMOVE(tree, key):
    guard = tree.collector.pin()
    defer guard.unpin()

    leaf, v_leaf = navigate_to_leaf_occ(root, key)
    prefetch_leaf_write(leaf)
    leaf.version.lock()

    // B-link forward walk under lock
    while key belongs in leaf.next:
        advance to next leaf (lock next, unlock current)

    slot = leaf.locked_find(key)
    if not found:
        leaf.version.release()
        return null

    if keylenx[slot] == LAYER:
        sublayer = values[slot].as_layer()
        leaf.version.release()
        key.shift()
        return REMOVE_AT_LAYER(sublayer, key)  // recurse

    value = leaf.values[slot]
    leaf.remove_slot(slot)                     // update permuter
    tree.shard_count.decrement()

    if leaf.is_empty():
        tree.coalesce_queue.enqueue(leaf)      // deferred cleanup
    leaf.version.release()

    return value
```

### Deferred Coalesce

Empty leaves are not immediately unlinked.  They are pushed onto the
**coalesce queue** (lock-free Treiber stack).  A periodic sweep:

1. Pops an entry.
2. Locks the leaf, re-verifies emptiness.
3. Sets the DELETED flag.
4. Unlinks from B-link chain (update predecessor's next).
5. Removes the child pointer from the parent internode.
6. Retires the leaf via EBR for safe reclamation.

Entries that fail verification (leaf was re-populated) are re-queued
up to MAX_REQUEUE (10) times.

---

## 7. Epoch-Based Reclamation (EBR)

Three-epoch scheme protecting lock-free readers from use-after-free:

```
Epoch Timeline:
  E-2         E-1         E (current)
  ----------------------------------------
  Reclaimable  Protected   Active
```

### Protocol

1. **pin()** -- thread announces participation in current epoch.
   Also drains the thread-local retire list for epoch <= E-2.
2. **Read/write operations** proceed freely.
3. **defer_retire(ptr)** -- queues ptr with current epoch tag.
4. **unpin()** -- thread announces completion.
5. **Epoch advance** -- when all pinned threads have observed
   the current epoch, global_epoch is incremented via CAS.
6. Items retired in epoch E are reclaimed when global epoch >= E + 2,
   guaranteeing no reader can still hold a reference.

---

## 8. Prefetch Strategy

During tree descent, cache-line prefetch hints reduce memory stall
cycles:

| Descent Phase         | What is Prefetched     | Locality | Intent |
|----------------------|------------------------|----------|--------|
| Before key search     | Internode CL1 (+64B)   | L1       | Read   |
| After child selected  | Child node CL0         | L1       | Read   |
| Before descending     | Grandchild CL0         | L1       | Read   |
| Before leaf lock      | Leaf CL0 + CL1         | L1       | Write  |
| B-link forward walk   | Next-next leaf (2-hop)  | L1       | Read   |

All prefetches are no-ops when enable_prefetch = false (comptime).

---

## 9. Sharded Counter for len()

The tree's element count uses a **16-shard counter** to avoid cache-line
bouncing on concurrent inserts/deletes.  Each shard is 128-byte aligned
to prevent false sharing.

```
Thread -> FNV-1a(thread_id) % 16 -> shard[i].atomic_add(1)
```

`len()` sums all 16 shards.  The result is approximate under concurrent
modification but eventually consistent.

---

## 10. Range Iteration

```
function RANGE_ALL(tree):
    guard = tree.collector.pin()
    cursor = navigate_to_leftmost_leaf(tree.root)
    key_buffer = CursorKey{}                   // accumulates prefix

    while cursor != null:
        v = cursor.version.stable()
        for slot in cursor.permuter.sorted_order():
            if cursor.keylenx[slot] == LAYER:
                push_layer(cursor, slot)       // descend into sublayer
                continue
            emit (reconstruct_key(key_buffer, cursor, slot), cursor.values[slot])
        if cursor.version.has_changed(v):
            restart from cursor                // OCC retry
        cursor = cursor.next                   // advance via B-link

    guard.unpin()
```

Iteration uses the same OCC retry loop as point queries.  The **layer
stack** tracks descent into sublayers, and CursorKey reconstructs the
full key by prepending the accumulated 8-byte prefix from each ancestor
layer.

---

## 11. Branch Hints

`@branchHint(.unlikely)` is applied to cold paths:

- OCC retry loops (has_changed branch)
- B-link forward walk (concurrent splits are rare)
- Lock contention spin paths

These hints allow the compiler and CPU branch predictor to optimise
for the common (fast) path.

---

## 12. Complexity Summary

| Operation     | Per-Layer Cost         | Total (L layers)          |
|--------------|------------------------|---------------------------|
| Get           | O(H * log F) + O(F)   | O(L * H * F)             |
| Put           | O(F) amortized         | O(L * F) amortized       |
| Remove        | O(F)                   | O(L * F)                 |
| Range (all N) | --                     | O(N) + retry overhead    |
| len()         | --                     | O(16) -- shard sum       |

Where:
- **L** = ceil(|key| / 8) -- number of trie layers
- **H** = B+ tree height within one layer (typically 2-3)
- **F** = FANOUT = 15 (constant)
- **N** = total entries in the tree

Since F and H are small constants, effective lookup cost is
**O(|key| / 8)** -- proportional to key length divided by 8.

Concurrent throughput scales linearly with threads for disjoint key
ranges.  Hot-key contention is bounded by the CAS spinlock retry cost.
