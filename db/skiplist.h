// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#define __STDC_LIMIT_MACROS

#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/atomic.h"
#include "util/random.h"

namespace leveldb {

class Arena;

template<typename Key, class Comparator, class Extractor>
class SkipList {
 private:
  struct Node;
  enum { kMaxHeight = 9 };

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Extractor ext, Arena* arena);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  // REQUIRES: external synchronization.
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const SkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  // Immutable after construction
  Comparator const compare_;
  Extractor const extractor_;
  Arena* const arena_;    // Arena used for allocations of nodes

  Node nodes_arr_[1 << 8];
  Node* nodes_;

  // Read/written only by Insert().
  port::Mutex rnd_mutex_;
  Random rnd_;

  bool IsSpacerNode(Node* n) const { return nodes_ <= n && n < nodes_ + (1 << 8); }

  Node* NewNode(const Key& key, unsigned height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return NULL if there is no such node.
  //
  // If prev is non-NULL, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev, Node** obs) const;

  // Return the latest non-spacer node that comes before "node"
  // Return NULL if there is no such node.
  Node* FindLessThan(Node* node) const;

  // Return the last node in the list.
  // Return NULL if list is spacers-only.
  Node* FindLast() const;

  // No copying allowed
  SkipList(const SkipList&);
  void operator=(const SkipList&);
};

// Implementation details follow
template<typename Key, class Comparator, class Extractor>
struct SkipList<Key,Comparator,Extractor>::Node {
  explicit Node() : cmp(), key() {
  }
  explicit Node(uint64_t c, const Key& k) : cmp(c), key(k) {
    for (unsigned i = 0; i < kMaxHeight; ++i) {
      atomic::store_32_nobarrier(&cmp_[i], UINT32_MAX);
      atomic::store_ptr_nobarrier(&next_[i], reinterpret_cast<Node*>(NULL));
    }
  }

  uint64_t const cmp;
  Key const key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(unsigned n) {
    assert(n < kMaxHeight);
    return atomic::load_ptr_acquire(&next_[n]);
  }
  void GetNext(unsigned n, uint32_t* c, Node** x) {
    *c = atomic::load_32_acquire(&cmp_[n]);
    *x = atomic::load_ptr_acquire(&next_[n]);
  }
  void SetNext(unsigned n, uint32_t c, Node* x) {
    assert(n < kMaxHeight);
    atomic::store_ptr_release(&next_[n], x);
    atomic::store_32_release(&cmp_[n], c);
  }
  bool CasNext(unsigned n, Node* old_node, Node* new_node) {
    using namespace atomic;
    if (compare_and_swap_ptr_release(&next_[n], old_node, new_node) == old_node) {
      uint32_t new_cmp = new_node->cmp >> 32;
      uint32_t expected = UINT32_MAX;
      while (expected > new_cmp) {
        expected = compare_and_swap_32_release(&cmp_[n], expected, new_cmp);
      }
      return true;
    } else {
      return false;
    }
  }
  void NoBarrier_GetNext(unsigned n, uint32_t* c, Node** x) { GetNext(n, c, x); }
  void NoBarrier_SetNext(unsigned n, uint32_t c, Node* x) { SetNext(n, c, x); }

 private:
  uint32_t cmp_[kMaxHeight];
  Node* next_[kMaxHeight];
};

template<typename Key, class Comparator, class Extractor>
typename SkipList<Key,Comparator,Extractor>::Node*
SkipList<Key,Comparator,Extractor>::NewNode(const Key& key, unsigned height) {
  char* mem = arena_->AllocateAligned(
      sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1));
  uint64_t cmp = extractor_(key);
  return new (mem) Node(cmp, key);
}

template<typename Key, class Comparator, class Extractor>
inline SkipList<Key,Comparator,Extractor>::Iterator::Iterator(const SkipList* list)
  : list_(list),
    node_(NULL) {
}

template<typename Key, class Comparator, class Extractor>
inline bool SkipList<Key,Comparator,Extractor>::Iterator::Valid() const {
  return node_ != NULL;
}

template<typename Key, class Comparator, class Extractor>
inline const Key& SkipList<Key,Comparator,Extractor>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template<typename Key, class Comparator, class Extractor>
inline void SkipList<Key,Comparator,Extractor>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
  while (node_ && list_->IsSpacerNode(node_)) {
    node_ = node_->Next(0);
  }
}

template<typename Key, class Comparator, class Extractor>
inline void SkipList<Key,Comparator,Extractor>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_);
  assert(!node_ || !list_->IsSpacerNode(node_));
}

template<typename Key, class Comparator, class Extractor>
inline void SkipList<Key,Comparator,Extractor>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, NULL, NULL);
  while (node_ && list_->IsSpacerNode(node_)) {
    node_ = node_->Next(0);
  }
}

template<typename Key, class Comparator, class Extractor>
inline void SkipList<Key,Comparator,Extractor>::Iterator::SeekToFirst() {
  node_ = list_->nodes_[0].Next(0);
  while (node_ && list_->IsSpacerNode(node_)) {
    node_ = node_->Next(0);
  }
}

template<typename Key, class Comparator, class Extractor>
inline void SkipList<Key,Comparator,Extractor>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
}

template<typename Key, class Comparator, class Extractor>
int SkipList<Key,Comparator,Extractor>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  rnd_mutex_.Lock();
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  rnd_mutex_.Unlock();
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template<typename Key, class Comparator, class Extractor>
bool SkipList<Key,Comparator,Extractor>::KeyIsAfterNode(const Key& key, Node* n) const {
  // NULL n is considered infinite
  assert(!IsSpacerNode(n));
  return (n != NULL) && (compare_(n->key, key) < 0);
}

template<typename Key, class Comparator, class Extractor>
typename SkipList<Key,Comparator,Extractor>::Node* SkipList<Key,Comparator,Extractor>::FindGreaterOrEqual(const Key& key, Node** prev, Node** obs)
    const {
  const uint64_t cmp = extractor_(key);
  Node* x = &nodes_[cmp >> 56];
  int level = kMaxHeight - 1;
  const uint32_t mask = cmp >> 32;
  while (true) {
    uint32_t c = 0;
    Node* next = NULL;
    x->GetNext(level, &c, &next);

    if (!IsSpacerNode(next) && (c < mask || KeyIsAfterNode(key, next))) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != NULL) prev[level] = x;
      if (obs != NULL) obs[level] = next;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        --level;
      }
    }
  }
}

template<typename Key, class Comparator, class Extractor>
typename SkipList<Key,Comparator,Extractor>::Node*
SkipList<Key,Comparator,Extractor>::FindLessThan(Node* node) const {
  Node* non_spacer = NULL;
  Node* x = &nodes_[0];
  while (x != node) {
    if (!IsSpacerNode(x)) {
      non_spacer = x;
    }
    x = x->Next(0);
  }
  return non_spacer;
  // XXX this block is broken, but could be more efficient if correct
#if 0
  Node* x = NULL;
  for (unsigned i = 0; i <= node->cmp >> 56; ++i) {
    if (!IsSpacerNode(nodes_[i].Next(0))) {
      x = &nodes_[i];
    }
  }
  x = x ? x : &nodes_[node->cmp >> 56];
  unsigned level = kMaxHeight - 1;
  const uint32_t mask = x->cmp >> 32;
  while (true) {
    uint32_t c = 0;
    Node* next = NULL;
    x->GetNext(level, &c, &next);
    if (!IsSpacerNode(x)) {
      non_spacer = x;
    }
    if (c >= mask && level > 0) {
      // Switch to next list
      --level;
    } else if (c > mask || next == node) {
      assert(level == 0);
      return non_spacer;
    } else {
      x = next;
    }
  }
#endif
}

template<typename Key, class Comparator, class Extractor>
typename SkipList<Key,Comparator,Extractor>::Node* SkipList<Key,Comparator,Extractor>::FindLast()
    const {
  Node* x = NULL;
  for (int i = (1 << 8) - 1; i >= 0; --i) {
    if (nodes_[i].Next(0) &&
        !IsSpacerNode(nodes_[i].Next(0))) {
      x = &nodes_[i];
      break;
    }
  }
  if (!x) {
    return NULL;
  }
  int level = kMaxHeight - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == NULL || IsSpacerNode(next)) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template<typename Key, class Comparator, class Extractor>
SkipList<Key,Comparator,Extractor>::SkipList(Comparator cmp, Extractor ext, Arena* arena)
    : compare_(cmp),
      extractor_(ext),
      arena_(arena),
      nodes_(NULL),
      rnd_mutex_(),
      rnd_(0xdeadbeef) {
  for (uint64_t idx = 0; idx < (1 << 8); ++idx) {
    uint64_t c = idx << 56;
    nodes_arr_[idx].~Node();
    new (&nodes_arr_[idx]) Node(c, Key());
  }
  nodes_ = nodes_arr_;
  for (uint64_t idx = 0; idx + 1 < (1 << 8); ++idx) {
    for (uint64_t level = 0; level < kMaxHeight; ++level) {
      nodes_[idx].SetNext(level, nodes_[idx + 1].cmp >> 32, nodes_ + idx + 1);
    }
  }
  for (uint64_t level = 0; level < kMaxHeight; ++level) {
    const uint64_t idx = (1 << 8) - 1;
    nodes_[idx].SetNext(level, UINT32_MAX, NULL);
  }
}

template<typename Key, class Comparator, class Extractor>
void SkipList<Key,Comparator,Extractor>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* obs[kMaxHeight];
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev, obs);

  // Our data structure does not allow duplicate insertion
  assert(x == NULL || IsSpacerNode(x) || !Equal(key, x->key));

  int height = RandomHeight();

  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    while (true) {
      Node* n = obs[i];
      uint32_t c = n ? n->cmp >> 32 : UINT32_MAX;
      x->NoBarrier_SetNext(i, c, n);
      if (c >= x->cmp >> 32 && prev[i]->CasNext(i, n, x)) {
        break;
      }

      // advance and retry the CAS
      while (true) {
        uint32_t c = 0;
        obs[i] = NULL;
        prev[i]->GetNext(i, &c, &obs[i]);
        if (!IsSpacerNode(obs[i]) &&
            (c < x->cmp >> 32 || KeyIsAfterNode(x->key, obs[i]))) {
          prev[i] = obs[i];
        } else {
          break;
        }
      }
    }
  }
}

template<typename Key, class Comparator, class Extractor>
bool SkipList<Key,Comparator,Extractor>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, NULL, NULL);
  if (x != NULL && !IsSpacerNode(x) && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb
