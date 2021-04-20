//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  std::scoped_lock bpm_slk{latch_};
  if(page_table_.find(page_id)!=page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    Page* page = pages_ + frame_id;
    page->pin_count_++;   
    return page;
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if(free_list_.empty()&&replacer_->Size()==0) {
    return nullptr;
  }
  frame_id_t frame_id = -1;
  Page *page = 0;
  if(!free_list_.empty()){
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = pages_ + frame_id;
  }
  else{
    replacer_->Victim(&frame_id);
    page = pages_ + frame_id;
    page_table_.erase(page->GetPageId());
  }

  // 2.     If R is dirty, write it back to the disk.
  if(page->is_dirty_){          
    disk_manager_->WritePage(page->GetPageId(),page->GetData());
  }

  // 3.     Delete R from the page table and insert P.
  page_table_[page_id] = frame_id;
  page->page_id_ = page_id;
  page->ResetMemory();

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  disk_manager_->ReadPage(page_id,page->data_);
  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {

  std::scoped_lock bpm_slk{latch_};

  if(page_table_.find(page_id)==page_table_.end()){//When the page is not found in the page_table_ or if found but pin_count = 0 return false.
    return false;
  }//If it is found -> initialize the frame_id and the page ->
  frame_id_t frame_id = page_table_[page_id];
  Page* page = pages_ + frame_id;
  if(page->pin_count_==0){
    return false;
  }
  
  if(is_dirty==true){
    (*page).is_dirty_ = true;
  }
  (*page).pin_count_-=1;

  if((*page).pin_count_==0){
    // unpin frame_id page.
    (*replacer_).Unpin(frame_id);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
    std::scoped_lock bpm_slk{latch_};
  
  if(page_table_.find(page_id)!=page_table_.end()){
   
  frame_id_t frame_id = page_table_[page_id];
  Page* page = pages_ + frame_id;
  disk_manager_->WritePage(page_id,page->GetData());
  page->is_dirty_ = false;
  return true;
  }
  else return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
   std::scoped_lock bpm_slk{latch_};
   // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  frame_id_t frame_id = -1;
  Page *page = nullptr;
  if(free_list_.empty()&&replacer_->Size()==0){
    return nullptr;
  }

   // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if(!free_list_.empty()){
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = pages_ + frame_id;
  }
  else{
    replacer_->Victim(&frame_id);
    page = pages_ + frame_id;
    page_table_.erase(page->page_id_);

    if(page->is_dirty_){
      disk_manager_->WritePage(page->page_id_,page->data_);
    }
  }

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  auto new_page_id = disk_manager_->AllocatePage();
  page_table_[new_page_id] = frame_id;
  page->ResetMemory();

  // 4.   Set the page ID output parameter. Return a pointer to P.
  page->page_id_ = new_page_id;
  *page_id = page->page_id_;
  page->is_dirty_ = true;
  page->pin_count_ = 1;
  return page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  std::scoped_lock bpm_slk{latch_};

  // 1.   Search the page table for the requested page (P).
  if(page_table_.find(page_id)==page_table_.end()) {
     return true;// 1.   If P does not exist, return true.
     }
  frame_id_t frame_id = page_table_[page_id];
  Page* page = pages_ + frame_id;

  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if(page->pin_count_>0){
    return false;
  }

  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list. 
  if(page->is_dirty_){
    FlushPageImpl(page_id);
  }
  page_table_.erase(page_id);
  page->ResetMemory();

  //metadata reset  and return true;
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  free_list_.push_front(frame_id);
  disk_manager_->DeallocatePage(page_id);//made sure that we have called DiskManager::DeallocatePage(Page_id)
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for(size_t i=0;i<pool_size_;i++){
    FlushPageImpl(pages_[i].page_id_);
  }
}

}  // namespace bustub
