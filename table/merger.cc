// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "hyperleveldb/comparator.h"
#include "hyperleveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
class MergingIterator : public Iterator {
 private:
  void ReinitializeComparisons();
  void ReinitializeCurrentComparison();
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        comparisons_(new uint64_t[n]),
        n_(n),
        comparisons_intialized_(false),
        current_(NULL),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    ReinitializeComparisons();
  }

  virtual ~MergingIterator() {
    delete[] children_;
  }

  virtual bool Valid() const {
    return (current_ != NULL);
  }

  virtual void SeekToFirst() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    ReinitializeComparisons();
    FindSmallest();
    direction_ = kForward;
  }

  virtual void SeekToLast() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    ReinitializeComparisons();
    FindLargest();
    direction_ = kReverse;
  }

  virtual void Seek(const Slice& target) {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    ReinitializeComparisons();
    FindSmallest();
    direction_ = kForward;
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
      ReinitializeComparisons();
      direction_ = kForward;
    }

    current_->Next();
    ReinitializeCurrentComparison();
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
      ReinitializeComparisons();
      direction_ = kReverse;
    }

    current_->Prev();
    ReinitializeCurrentComparison();
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

  virtual Status status() const {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  uint64_t* comparisons_;
  int n_;
  bool comparisons_intialized_;
  IteratorWrapper* current_;

  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  Direction direction_;
};

void MergingIterator::ReinitializeComparisons() {
  for (int i = 0; i < n_; ++i) {
    if (children_[i].Valid()) {
      comparisons_[i] = comparator_->KeyNum(children_[i].key());
    }
  }
}

void MergingIterator::ReinitializeCurrentComparison() {
  if (current_->Valid()) {
    comparisons_[current_ - children_] = comparator_->KeyNum(current_->key());
  }
}

void MergingIterator::FindSmallest() {
  size_t smallest_idx = 0;
  IteratorWrapper* smallest = NULL;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == NULL) {
        smallest = child;
        smallest_idx = i;
      } else if (comparisons_[i] < comparisons_[smallest_idx] ||
                 (comparisons_[i] == comparisons_[smallest_idx] &&
                  comparator_->Compare(child->key(), smallest->key()) < 0)) {
        smallest = child;
        smallest_idx = i;
      }
    }
  }
  current_ = smallest;
}

void MergingIterator::FindLargest() {
  size_t largest_idx = 0;
  IteratorWrapper* largest = NULL;
  for (int i = n_-1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == NULL) {
        largest = child;
        largest_idx = i;
      } else if (comparisons_[i] > comparisons_[largest_idx] ||
                 (comparisons_[i] == comparisons_[largest_idx] &&
                  comparator_->Compare(child->key(), largest->key()) > 0)) {
        largest = child;
        largest_idx = i;
      }
    }
  }
  current_ = largest;
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
