//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *,
                int, BufferPoolManager *);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const ;

  bool operator!=(const IndexIterator &itr) const;

 private:
  // add your own private member variables here
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_;
  int index_;
  BufferPoolManager *buff_pool_manager_;
};

}  // namespace bustub
