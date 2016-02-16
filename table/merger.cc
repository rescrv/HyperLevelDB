// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include <algorithm>

#include "hyperleveldb/comparator.h"
#include "hyperleveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
class MergingIterator;

struct HeapComparator {
  HeapComparator(MergingIterator* mi) : mi_(mi) {}
  HeapComparator(const HeapComparator& other) : mi_(other.mi_) {}

  MergingIterator* mi_;

  bool operator () (unsigned lhs, unsigned rhs) const;

  private:
    HeapComparator& operator = (const HeapComparator&);
};

class MergingIterator : public Iterator {
 private:
  void ReinitializeComparisons();
  void PopCurrentComparison();
  void PushCurrentComparison();
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        comparisons_(new uint64_t[n]),
        heap_(new unsigned[n]),
        heap_sz_(0),
        n_(n),
        comparisons_intialized_(false),
        current_(NULL),
        status_(),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    ReinitializeComparisons();
  }

  virtual ~MergingIterator() {
    delete[] children_;
    delete[] comparisons_;
    delete[] heap_;
  }

  virtual bool Valid() const {
    return (current_ != NULL);
  }

  virtual void SeekToFirst() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    direction_ = kForward;
    ReinitializeComparisons();
    FindSmallest();
  }

  virtual void SeekToLast() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    direction_ = kReverse;
    ReinitializeComparisons();
    FindLargest();
  }

  virtual void Seek(const Slice& target) {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    direction_ = kForward;
    ReinitializeComparisons();
    FindSmallest();
  }

  virtual void Next() {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
      ReinitializeComparisons();
    }

    PopCurrentComparison();
    current_->Next();
    PushCurrentComparison();
    FindSmallest();
  }

  virtual void Prev() {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
      ReinitializeComparisons();
    }

    PopCurrentComparison();
    current_->Prev();
    PushCurrentComparison();
    FindLargest();
  }

  virtual Slice key() const {
    assert(Valid());
    return current_->key();
  }

  virtual Slice value() const {
    assert(Valid());
    return current_->value();
  }

  virtual const Status& status() const {
    // XXX this value can easily be cached
    for (int i = 0; i < n_; i++) {
      if (!children_[i].status().ok()) {
        return children_[i].status();
      }
    }
    return status_;
  }

 private:
  MergingIterator(const MergingIterator&);
  MergingIterator& operator = (const MergingIterator&);
  friend class HeapComparator;
  void FindSmallest();
  void FindLargest();

  const Comparator* comparator_;
  IteratorWrapper* children_;
  uint64_t* comparisons_;
  unsigned* heap_;
  size_t heap_sz_;
  int n_;
  bool comparisons_intialized_;
  IteratorWrapper* current_;
  Status status_;

  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  Direction direction_;
};

bool HeapComparator::operator ()(unsigned lhs, unsigned rhs) const {
  if (mi_->direction_ == MergingIterator::kForward) {
    std::swap(lhs, rhs);
  }
  return mi_->comparisons_[lhs] < mi_->comparisons_[rhs] ||
         (mi_->comparisons_[lhs] == mi_->comparisons_[rhs] &&
          mi_->comparator_->Compare(mi_->children_[lhs].key(), mi_->children_[rhs].key()) < 0);
}

void MergingIterator::ReinitializeComparisons() {
  heap_sz_ = 0;
  for (int i = 0; i < n_; ++i) {
    if (children_[i].Valid()) {
      comparisons_[i] = comparator_->KeyNum(children_[i].key());
      heap_[heap_sz_] = i;
      ++heap_sz_;
    }
  }
  HeapComparator hc(this);
  std::make_heap(heap_, heap_ + heap_sz_, hc);
}

void MergingIterator::PopCurrentComparison() {
  unsigned idx = current_ - children_;
  assert(heap_[0] == idx);
  HeapComparator hc(this);
  std::pop_heap(heap_, heap_ + heap_sz_, hc);
  --heap_sz_;
}

void MergingIterator::PushCurrentComparison() {
  unsigned idx = current_ - children_;
  assert(&children_[idx] == current_);
  if (current_->Valid()) {
    comparisons_[idx] = comparator_->KeyNum(current_->key());
    heap_[heap_sz_] = idx;
    ++heap_sz_;
    HeapComparator hc(this);
    std::push_heap(heap_, heap_ + heap_sz_, hc);
  }
}

void MergingIterator::FindSmallest() {
  assert(direction_ == kForward);
  if (heap_sz_ > 0) {
    current_ = &children_[heap_[0]];
  } else {
    current_ = NULL;
  }
}

void MergingIterator::FindLargest() {
  assert(direction_ == kReverse);
  if (heap_sz_ > 0) {
    current_ = &children_[heap_[0]];
  } else {
    current_ = NULL;
  }
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, n);
  }
}

}  // namespace leveldb
