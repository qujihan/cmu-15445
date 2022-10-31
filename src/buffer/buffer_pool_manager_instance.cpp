//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  const std::lock_guard<std::mutex> lock(latch_);
  // page_it->first 是 page_id page_it->second 是 frame_id
  auto page_it = page_table_.find(page_id);
  if (page_it == page_table_.end()) {
    return false;
  }
  Page *page = &pages_[page_it->second];
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  const std::lock_guard<std::mutex> lcok(latch_);
  Page *page = nullptr;
  for (auto page_it : page_table_) {
    page = &pages_[page_it.second];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  const std::lock_guard<std::mutex> lock(latch_);
  // 0.   Make sure you call AllocatePage!

  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if (replacer_->Size() == 0 && free_list_.empty()) {
    return nullptr;
  }

  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  int frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&frame_id);
  }

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  Page *page = &pages_[frame_id];
  // 如果这个页框曾经有页面 将其清空
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  page_table_.erase(page->GetPageId());
  // 设置page的元数据
  *page_id = AllocatePage();
  page->page_id_ = *page_id;
  page->ResetMemory();
  page->pin_count_ = 0;
  page->pin_count_ += 1;
  page->is_dirty_ = false;

  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_table_[page->GetPageId()] = frame_id;

  // std::cout<< "page id:" << page->GetPageId()<<"  page date"<<page->GetData()<<std::endl;
  return page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  const std::lock_guard<std::mutex> lock(latch_);
  // 1.     Search the page table for the requested page (P).
  Page *page = nullptr;
  auto page_it = page_table_.find(page_id);

  // 1.1    If P exists, pin it and return it immediately.
  if (page_it != page_table_.end()) {
    page = &pages_[page_it->second];
    page->pin_count_ += 1;
    replacer_->Pin(page_it->second);
    return page;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (replacer_->Size() == 0 && free_list_.empty()) {
    return nullptr;
  }

  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&frame_id);
  }
  page = &pages_[frame_id];

  // 2.     If R is dirty, write it back to the disk.
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }

  // 3.     Delete R from the page table and insert P.
  page_table_.erase(page->GetPageId());
  page_table_[page_id] = frame_id;

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  disk_manager_->ReadPage(page_id, page->GetData());
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->pin_count_ += 1;
  page->page_id_ = page_id;

  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  const std::lock_guard<std::mutex> lock(latch_);
  auto page_it = page_table_.find(page_id);
  if (page_it == page_table_.end()) {
    return true;
  }

  Page *page = &pages_[page_it->second];
  if (page->pin_count_ != 0) {
    return false;
  }

  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }

  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;

  free_list_.emplace_back(page_it->second);
  replacer_->Pin(page_it->second);
  page_table_.erase(page_it);

  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  const std::lock_guard<std::mutex> lock(latch_);
  auto page_it = page_table_.find(page_id);
  if (page_it == page_table_.end()) {
    return false;
  }

  Page *page = &pages_[page_it->second];
  page->is_dirty_ = is_dirty || page->is_dirty_;
  page->pin_count_ -= 1;
  if (page->pin_count_ == 0) {
    replacer_->Unpin(page_it->second);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
