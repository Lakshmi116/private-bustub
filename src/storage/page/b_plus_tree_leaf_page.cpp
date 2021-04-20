//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         **DO NO SHARE PUBLICLY**
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include <string.h>
#include <bits/stdc++.h>

#include "common/exception.h"
#include "common/rid.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {

/***************************
 * HELPER METHODS AND UTILITIES
 ***************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::
Init(page_id_t page_id, page_id_t parent_id) {

  // set page type
  SetPageType(IndexPageType::LEAF_PAGE);
  // set current size: 1 for the first invalid key
  SetSize(0);
  // set page id
  SetPageId(page_id);
  // set parent id
  SetParentPageId(parent_id);
  // set next page id
  SetNextPageId(INVALID_PAGE_ID);

  // set max page size, header is 28bytes
  int size = (PAGE_SIZE - sizeof(BPlusTreeLeafPage))/
      (sizeof(KeyType) + sizeof(ValueType));
  SetMaxSize(size);

}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::
GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::
SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::
KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(key, array[i].first) <= 0) {
      return i;
    }
  }
  return GetSize();
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::
KeyAt(int index) const {
  // replace with your own code
  assert(0 <= index && index < GetSize());
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::
GetItem(int index) {
  // replace with your own code
  assert(0 <= index && index < GetSize());
  return array[index];
}

/***************************
 * INSERTION
 ***************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::
Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {

   if (GetSize() == 0 || comparator(key, KeyAt(GetSize() - 1)) > 0) {
    array[GetSize()] = {key, value};
  } else if (comparator(key, array[0].first) < 0) {

    memmove(static_cast<void*>(array + 1), static_cast<void*>(array), static_cast<size_t>(GetSize()*sizeof(MappingType)));
    array[0] = {key, value};
  } else {
    int low = 0, high = GetSize() - 1, mid;
    while (low < high && low + 1 != high) {
      mid = low + (high - low)/2;
      if (comparator(key, array[mid].first) < 0) {
        high = mid;
      } else if (comparator(key, array[mid].first) > 0) {
        low = mid;
      } else {
        // only support unique key
        assert(0);
      }
    }
    memmove(static_cast<void*>(array + high + 1), static_cast<void*>(array + high), static_cast<size_t>((GetSize() - high)*sizeof(MappingType)));
    array[high] = {key, value};
  }

  IncreaseSize(1);
  assert(GetSize() <= GetMaxSize());
  return GetSize();
}

/***************************
 * SPLIT
 ***************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::
MoveHalfTo(BPlusTreeLeafPage *recipient, __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {

  // at least have some key-value pairs
  assert(GetSize() > 0);

  int size = GetSize()/2;
  MappingType *src = array + GetSize() - size;
  recipient->CopyNFrom(src, size);
  IncreaseSize(-1*size);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::
CopyNFrom(MappingType *items, int size) {
   assert(IsLeafPage() && GetSize() == 0);
  for (int i = 0; i < size; ++i) {
    array[i] = *items++;
  }
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::
CopyHalfFrom(MappingType *items, int size) {
  // must be empty leaf page
  assert(IsLeafPage() && GetSize() == 0);
  for (int i = 0; i < size; ++i) {
    array[i] = *items++;
  }
  IncreaseSize(size);
}


/***************************
 * LOOKUP
 ***************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::
Lookup(const KeyType &key,ValueType &value, const KeyComparator &comparator) const {
 
  if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 ||
      comparator(key, KeyAt(GetSize() - 1)) > 0) {
    return false;
  }
  // binary search
  int low = 0, high = GetSize() - 1, mid;
  while (low <= high) {
    mid = low + (high - low)/2;
    if (comparator(key, KeyAt(mid)) > 0) {
      low = mid + 1;
    } else if (comparator(key, KeyAt(mid)) < 0) {
      high = mid - 1;
    } else {
      value = array[mid].second;
      return true;
    }
  }
  return false;
}

/***************************
 * REMOVE
 ***************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::
RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
   if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 ||
      comparator(key, KeyAt(GetSize() - 1)) > 0) {
    return GetSize();
  }

  // binary search
  int low = 0, high = GetSize() - 1, mid;
  while (low <= high) {
    mid = low + (high - low)/2;
    if (comparator(key, KeyAt(mid)) > 0) {
      low = mid + 1;
    } else if (comparator(key, KeyAt(mid)) < 0) {
      high = mid - 1;
    } else {
      // delete
      memmove(static_cast<void*>(array + mid), static_cast<void*>(array + mid + 1), static_cast<size_t>((GetSize() - mid - 1)*sizeof(MappingType)));
      IncreaseSize(-1);
      break;
    }
  }
  return GetSize();
}

/***************************
 * MERGE
 ***************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::
MoveAllTo(BPlusTreeLeafPage *recipient,int, BufferPoolManager *) {
   recipient->CopyAllFrom(array, GetSize());
  recipient->SetNextPageId(GetNextPageId());
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::
CopyAllFrom(MappingType *items, int size) {
  assert(GetSize() + size <= GetMaxSize());
  auto start = GetSize();
  for (int i = 0; i < size; ++i) {
    array[start + i] = *items++;
  }
  IncreaseSize(size);
}

/***************************
 * REDISTRIBUTE
 ***************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::
MoveFirstToEndOf(BPlusTreeLeafPage *recipient, 
                 BufferPoolManager *buffer_pool_manager){

  MappingType pair = GetItem(0);
  IncreaseSize(-1);
  memmove(static_cast<void*>(array), static_cast<void*>(array + 1), static_cast<size_t>(GetSize()*sizeof(MappingType)));

  recipient->CopyLastFrom(pair);

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
   // throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while MoveFirstToEndOf");
  }
  // update relevant key & value pair in parent
  auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()),KeyComparator> *>(page->GetData());

  // replace key in parent with the moving one
  parent->SetKeyAt(parent->ValueIndex(GetPageId()), pair.first);

  // unpin parent when we are done
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::
CopyLastFrom(const MappingType &item){
  assert(GetSize() + 1 <= GetMaxSize());
  array[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::
MoveLastToFrontOf(BPlusTreeLeafPage *recipient, int parentIndex, 
                  BufferPoolManager *buffer_pool_manager) {

   MappingType pair = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::
CopyFirstFrom(const MappingType &item, int parentIndex, 
              BufferPoolManager *buffer_pool_manager){
   assert(GetSize() + 1 < GetMaxSize());
  memmove(static_cast<void*>(array + 1), static_cast<void*>(array), GetSize()*sizeof(MappingType));
  IncreaseSize(1);
  array[0] = item;

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    //throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyFirstFrom");
  }
  // get parent
  auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, decltype(GetPageId()), KeyComparator> *>(page->GetData());

  // replace with moving key
  parent->SetKeyAt(parentIndex, item.first);

  // unpin when are done
  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub