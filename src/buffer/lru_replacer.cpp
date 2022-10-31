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

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  const std::lock_guard<std::mutex> lock(frame_mutex_);
  if (frames_.empty()) {
    return false;
  }
  *frame_id = frames_.front();
  frames_.pop_front();
  frames_table_.erase(*frame_id);
  return true;
}

// Pin的意思是将页框使用了 换句话说也就是buffer_pool拿去用了 要从lru中去除
void LRUReplacer::Pin(frame_id_t frame_id) {
  const std::lock_guard<std::mutex> lock(frame_mutex_);
  auto it = frames_table_.find(frame_id);
  if (it == frames_table_.end()) {
    return;
  }
  frames_.erase(it->second);
  frames_table_.erase(frame_id);
}

// Unpin的意思是将页框还回来 buffer_pool不使用了 要加入lru中
void LRUReplacer::Unpin(frame_id_t frame_id) {
  const std::lock_guard<std::mutex> lock(frame_mutex_);
  auto it = frames_table_.find(frame_id);
  if (it != frames_table_.end()) {
    return;
  }
  frames_.push_back(frame_id);
  frames_table_.emplace(frame_id, std::prev(frames_.end()));
}

size_t LRUReplacer::Size() {
  const std::lock_guard<std::mutex> lock(frame_mutex_);
  return frames_.size();
}

}  // namespace bustub
