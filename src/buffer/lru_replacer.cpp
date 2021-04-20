//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages(num_pages) { lru_map.reserve(this->num_pages); }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(lru_lock);
  bool flag = true;
  /* Since we dont have anything to update in this case*/
  /* Else we simply have to remove the least recently used page,
   which is indeed in the back of the list and reflect the same in the map
  */
  if (Size() == 0) {
    flag = false;
  } else {
    *frame_id = lru_bucket_list.back();
    lru_map.erase(*frame_id);
    lru_bucket_list.pop_back();//popped out the lru page.
  }
  return flag;//as the task is successful true returned else, false returns when size=0.
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(lru_lock);
  if (lru_map.find(frame_id) == lru_map.end()) {
    return;//nothing to do
  } else {
    lru_bucket_list.erase(lru_map[frame_id]);
    lru_map.erase(frame_id);//we have to exempt this page from removal, hence erasing this from the map and the list.
    return;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(lru_lock);
  auto lru_map_iterator = lru_map.find(frame_id);
  if (lru_map_iterator != lru_map.end()) {
    return;//nothing to do
  } else {
    lru_bucket_list.push_front(frame_id);
    lru_map[frame_id] = lru_bucket_list.begin();//as this is now the most recently used page we have to add this to the front of the list
    return;
  }
  /*
  Note: if you are arranging the data of pages in reverse direction 
  then the removal operation is from the front, 
  unpin will be pushig back the page at the back
  */
}

size_t LRUReplacer::Size() { return lru_map.size(); }

}  // namespace bustub
