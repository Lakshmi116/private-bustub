//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         **DO NO SHARE PUBLICLY**
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/***************************
 * HELPER METHODS AND UTILITIES
 ***************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::
Init(page_id_t page_id, page_id_t parent_id) {

   // set page type
  SetPageType(IndexPageType::INTERNAL_PAGE);
  // set current size: 1 for the first invalid key
  SetSize(1);
  // set page id
  SetPageId(page_id);
  // set parent id
  SetParentPageId(parent_id);

  // set max page size, header is 24bytes
  int size = (PAGE_SIZE - sizeof(BPlusTreeInternalPage))/
      (sizeof(KeyType) + sizeof(ValueType));
  SetMaxSize(size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::
KeyAt(int index) const {
  // replace with your own code
  assert(0<=index && index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::
SetKeyAt(int index, const KeyType &key) {

  assert(0<=index && index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::
ValueIndex(const ValueType &value) const {

  for (int i = 0; i < GetSize(); ++i) {
    if (array[i].second == value) {
      return i;
    }
  }
  return GetSize();
}


/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::
ValueAt(int index) const { 
  assert(0<=index && index < GetSize());
  return array[index].second;
 }

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::
SetValueAt(int index, const ValueType &value) {
  assert(0 <= index && index < GetSize());
  array[index].second = value;
}

/***************************
 * LOOKUP
 ***************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::
Lookup(const KeyType &key, const KeyComparator &comparator) const {
  
  assert(GetSize() > 1);
  if (comparator(key, array[1].first) < 0) {
    return array[0].second;
  } else if (comparator(key, array[GetSize() - 1].first) >= 0) {
    return array[GetSize() - 1].second;
  }

  int low = 1, high = GetSize() - 1, mid;
  while (low < high && low + 1 != high) {
    mid = low + (high - low)/2;
    if (comparator(key, array[mid].first) < 0) {
      high = mid;
    } else if (comparator(key, array[mid].first) > 0) {
      low = mid;
    } else {
      return array[mid].second;
    }
  }
  return array[low].second;

}

/***************************
 * INSERTION
 ***************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::
PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, 
                const ValueType &new_value) {

  assert(GetSize() == 1);
  array[0].second = old_value;
  array[1] = {new_key, new_value};
  IncreaseSize(1);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::
InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                const ValueType &new_value) {

   for (int i = GetSize(); i > 0; --i) {
    if (array[i - 1].second == old_value) {
      array[i] = {new_key, new_value};
      IncreaseSize(1);
      break;
    }
    array[i] = array[i - 1];
  }
  return GetSize();
}

/***************************
 * SPLIT
 ***************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::
MoveHalfTo(BPlusTreeInternalPage *recipient, 
           BufferPoolManager *buffer_pool_manager){

  auto half = (GetSize() + 1)/2;
  recipient->CopyHalfFrom(array + GetSize() - half, half, buffer_pool_manager);

  // update parent page id of all children
  for (auto index = GetSize() - half; index < GetSize(); ++index) {
    auto *page = buffer_pool_manager->FetchPage(ValueAt(index));
    if (page == nullptr) {
      //throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyLastFrom");
    }
    auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child->SetParentPageId(recipient->GetPageId());

    assert(child->GetParentPageId() == recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  }
  IncreaseSize(-1*half);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::
CopyHalfFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager){
  assert(!IsLeafPage() && GetSize() == 1 && size > 0);
  for (int i = 0; i < size; ++i) {
    array[i] = *items++;
  }
  IncreaseSize(size - 1);
}

/***************************
 * REMOVE
 ***************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::
Remove(int index){

  assert(0 <= index && index < GetSize());
  for (int i = index; i < GetSize() - 1; ++i) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::
RemoveAndReturnOnlyChild(){

  IncreaseSize(-1);
  assert(GetSize() == 1);
  return ValueAt(0);
}
/***************************
 * MERGE
 ***************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */



INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::
MoveAllTo(BPlusTreeInternalPage *recipient, int index_in_parent, BufferPoolManager *buffer_pool_manager){

  //int index_in_parent = parent_index;
  
  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  //if (page == nullptr) {throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while MoveAllTo");}
  auto *parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

  // the separation key from parent
  SetKeyAt(0, parent->KeyAt(index_in_parent));

  // assumption: current page is at the right hand of recipient
  assert(parent->ValueAt(index_in_parent) == GetPageId());

  // unpin parent page
  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);

  recipient->CopyAllFrom(array, GetSize(), buffer_pool_manager);

  // update parent page id of all children
  for (auto index = 0; index < GetSize(); ++index) {
    auto *page = buffer_pool_manager->FetchPage(ValueAt(index));
    if (page == nullptr) {
      //throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyLastFrom");
    }
    auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child->SetParentPageId(recipient->GetPageId());

    assert(child->GetParentPageId() == recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  }

}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::
CopyAllFrom(MappingType *items, int size,
            BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + size <= GetMaxSize());
  int start = GetSize();
  for (int i = 0; i < size; ++i) {
    array[start + i] = *items++;
  }
  IncreaseSize(size);
}

/***************************
 * REDISTRIBUTE
 ***************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::
MoveFirstToEndOf(BPlusTreeInternalPage *recipient,
                 BufferPoolManager *buffer_pool_manager) {

  assert(GetSize() > 1);
  MappingType pair{KeyAt(1), ValueAt(0)};
  page_id_t child_page_id = ValueAt(0);
  SetValueAt(0, ValueAt(1));
  Remove(1);

  // delegate to helper function
  recipient->CopyLastFrom(pair, buffer_pool_manager);

  // update child parent page id
  auto *page = buffer_pool_manager->FetchPage(child_page_id);
  if (page == nullptr) {
   // throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyLastFrom");
  }
  auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(recipient->GetPageId());

  assert(child->GetParentPageId() == recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);

}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::
CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager){
  assert(GetSize() + 1 <= GetMaxSize());

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    //throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyLastFrom");
  }
  auto parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

  auto index = parent->ValueIndex(GetPageId());
  auto key = parent->KeyAt(index + 1);

  array[GetSize()] = {key, pair.second};
  IncreaseSize(1);
  parent->SetKeyAt(index + 1, pair.first);

  // unpin when we are done
  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::
MoveLastToFrontOf(BPlusTreeInternalPage *recipient, int parent_index,
                  BufferPoolManager *buffer_pool_manager){

   assert(GetSize() > 1);
  IncreaseSize(-1);
  MappingType pair = array[GetSize()];
  page_id_t child_page_id = pair.second;

  // delegate
  recipient->CopyFirstFrom(pair, parent_index, buffer_pool_manager);

  // update child parent page id
  auto *page = buffer_pool_manager->FetchPage(child_page_id);
  if (page == nullptr) {
    //throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyLastFrom");
  }
  auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(recipient->GetPageId());

  assert(child->GetParentPageId() == recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);

}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::
CopyFirstFrom(const MappingType &pair,int parent_index,
               BufferPoolManager *buffer_pool_manager){
  assert(GetSize() + 1 < GetMaxSize());

  auto *page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    //throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while CopyFirstFrom");
  }
  auto parent = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

  auto key = parent->KeyAt(parent_index);



  // set parent key to the last of current page
  parent->SetKeyAt(parent_index, pair.first);

  InsertNodeAfter(array[0].second, key, array[0].second);
  array[0].second = pair.second;

  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}



// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub