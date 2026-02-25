# Algorithms

This document describes the core algorithms used in the Masstree
implementation, with pseudocode and complexity analysis.

---

## 1. Key Slicing

Given a variable-length byte key `K` and a trie depth `d` (0-indexed):

```
slice = K[d*8 .. (d+1)*8]     // zero-padded if key is short
u64   = big_endian(slice)
```

**Why big-endian?**  Big-endian encoding ensures that the numeric
comparison of `u64` values matches the lexicographic comparison of
the original byte strings.  For example:

```
"apple___" → 0x6170706C655F5F5F
"banana__" → 0x62616E616E615F5F
0x6170… < 0x6261…  ✓  (matches "apple" < "banana")
```

**Complexity:** O(1) per slice extraction.

---

## 2. Lookup (`get`)

```
function GET(layer, key):
    ks ← make_slice(key, layer.depth)

    leaf ← find_leaf(layer.root, ks)   // descend through interior nodes
    if leaf is null:
        return null

    (idx, found) ← leaf.find_pos(ks)
    if not found:
        return null

    entry ← leaf.entries[idx]
    match entry.value:
        Value(v):
            if entry.full_key == key: return v
            else: return null          // different full key
        Link(sublayer) → return GET(sublayer, key)   // recurse
```

### `find_leaf`

Starting from an interior node, follow `children[find_child_idx(ks)]`
until a leaf is reached.

### `find_pos` (within a leaf)

Linear scan over the (small, ≤15) sorted entries.  Entries are compared
by `key_slice` (u64) only.  Each key_slice has at most one entry per
leaf; keys that share a slice are disambiguated via sublayers.

**Complexity:**

| Component           | Cost         |
|---------------------|-------------|
| Per interior level  | O(F) scan   |
| Per leaf            | O(F) scan   |
| Across trie layers  | O(L) layers |
| **Total**           | O(L · H · F) |

Where L = ⌈|key|/8⌉, H = B⁺ tree height, F = FANOUT (constant 15).

---

## 3. Insertion (`put`)

```
function PUT(layer, key, value):
    ks ← make_slice(key, layer.depth)

    if layer.root is Empty:
        create leaf with single entry (ks, key, value)
        return

    leaf ← find_leaf(layer.root, ks)
    (idx, found) ← leaf.find_pos(ks)

    if found:
        entry ← leaf.entries[idx]
        match entry.value:
            Value(_):
                if entry.full_key == key:
                    entry.value ← value        // UPDATE in place
                else:
                    // Key-slice collision with different full key
                    sub ← new Layer(depth + 1)
                    sub.PUT(entry.full_key, entry.value)
                    sub.PUT(key, value)
                    entry.value ← Link(sub)    // PROMOTE to sublayer
            Link(sub):
                sub.PUT(key, value)            // RECURSE into sublayer
    else:
        if leaf is not full:
            leaf.insert_at(idx, new_entry)
        else:
            (splitKey, newLeaf) ← split_leaf(leaf, idx, new_entry)
            propagate splitKey upward          // may cascade
```

### 3a. Leaf Split

When a leaf is full (FANOUT entries) and a new entry must be added:

1. Build a temporary array of FANOUT + 1 entries (existing + new),
   already in sorted order.
2. Choose `mid = FANOUT / 2` as the split point.
3. **Left leaf:** entries `[0 .. mid)`.
4. **Right leaf:** entries `[mid .. FANOUT+1)`.
5. **Promoted separator:** `entries[mid].key_slice`.
6. Maintain doubly-linked list pointers.

```
Before:  [e0 e1 e2 … e14]  +  new_entry
After:   [e0 … e6]  |separator→  [e7 … e14 new]
         (left)                   (right)
```

### 3b. Interior Split

When an interior node is full and a new separator must be inserted:

1. Build temporary array of FANOUT + 1 keys and FANOUT + 2 children.
2. Choose `mid = FANOUT / 2`.
3. **Left node:** keys `[0..mid)`, children `[0..mid]`.
4. **Right node:** keys `[mid+1..FANOUT+1)`, children `[mid+1..FANOUT+2)`.
5. **Promoted key:** `keys[mid]` (pushed up to parent).

If the root itself splits, a new root interior node is created with a
single separator and two children.

### 3c. Sublayer Creation

When two keys collide on the same 8-byte slice at depth `d`:

1. Create a new `Layer` at depth `d + 1`.
2. Insert both the existing key and the new key into the sublayer.
3. Replace the leaf entry's `.value` with `.link(sublayer)`.

This recursion continues for as many 8-byte chunks as needed, bounded
by `⌈|key| / 8⌉`.

**Complexity:** O(F) per split (shifting entries).  Amortized O(1) per
insert.

---

## 4. Deletion (`remove`)

```
function REMOVE(layer, key):
    ks ← make_slice(key, layer.depth)

    leaf ← find_leaf(layer.root, ks)
    if leaf is null: return false

    (idx, found) ← leaf.find_pos(ks)
    if not found: return false

    entry ← leaf.entries[idx]
    match entry.value:
        Value(_):
            if entry.full_key != key: return false
            free(entry.full_key)
            leaf.remove_at(idx)        // shift entries left
            return true
        Link(sublayer):
            return sublayer.REMOVE(key)
```

**Note:** This implementation does **not** perform node merging or
redistribution after deletion.  Nodes may become underfull.  This is a
deliberate simplicity trade-off, acceptable for insert-heavy workloads.

**Complexity:** O(F) per leaf (shifting).

---

## 5. Trie Layer Transition — Why It Works

Consider inserting keys `"abcdefghXXX"` and `"abcdefghYYY"`:

```
Layer 0:
  makeSlice("abcdefghXXX", 0) = makeSlice("abcdefghYYY", 0)
  → same u64!  Collision.

  → Create Layer 1:
      makeSlice("abcdefghXXX", 1) = u64("XXX\0\0\0\0\0")
      makeSlice("abcdefghYYY", 1) = u64("YYY\0\0\0\0\0")
      → different.  Stored normally in Layer 1's B⁺ tree.
```

This is the core insight: collisions at one depth are resolved at the
next depth, and the recursion is bounded by key length.

---

## 6. Complexity Summary

| Operation | Per trie layer    | Total (L layers)   |
|-----------|-------------------|--------------------|
| Get       | O(H · F)          | O(L · H · F)       |
| Put       | O(F) amortized    | O(L · F) amortized |
| Remove    | O(F)              | O(L · F)           |

Where:
- **L** = ⌈|key| / 8⌉ — number of trie layers
- **H** = B⁺ tree height within one layer (typically 2–3)
- **F** = FANOUT = 15 (constant)
- **N** = total entries in the tree

Since F and typical H are small constants, the effective lookup cost is
essentially **O(|key| / 8)** — proportional to key length divided by 8.
