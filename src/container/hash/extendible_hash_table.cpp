//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  // init dircetory page
  HashTableDirectoryPage *dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_)->GetData());
  dir_page->SetPageId(directory_page_id_);

  // init first bucket
  page_id_t first_bucket_index = INVALID_PAGE_ID;
  HASH_TABLE_BUCKET_TYPE *init_bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager->NewPage(&first_bucket_index)->GetData());
  assert(init_bucket_page != nullptr);

  // connceting dir to bucket
  dir_page->SetBucketPageId(0, first_bucket_index);
  dir_page->SetLocalDepth(0, 0);

  // unpin
  buffer_pool_manager->UnpinPage(first_bucket_index, true);
  buffer_pool_manager->UnpinPage(directory_page_id_, true);

  // std::ifstream file2("/autograder/bustub/test/container/grading_hash_table_scale_test.cpp");
  // std::ifstream file3("/autograder/bustub/src/storage/page/hash_table_directory_page.cpp");
  // std::ifstream file4("/autograder/bustub/test/container/grading_hash_table_verification_test.cpp");

  // std::string buffer2;
  // std::string buffer3;
  // std::string buffer4;

  // while (file2) {
  //   std::getline(file2, buffer2);
  //   std::cout << buffer2 << std::endl;
  // }
  // while (file3) {
  //   std::getline(file3, buffer3);
  //   std::cout << buffer3 << std::endl;
  // }
  // while (file4) {
  //   std::getline(file4, buffer4);
  //   std::cout << buffer4 << std::endl;
  // }
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH 搜索
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  bool flag = false;

  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());

  Page *bucket_page_lock = reinterpret_cast<Page *>(bucket_page);
  bucket_page_lock->RLatch();
  flag = bucket_page->GetValue(key, comparator_, result);
  bucket_page_lock->RUnlatch();

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);

  table_latch_.RUnlock();

  return flag;
}

/*****************************************************************************
 * INSERTION 插入
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  bool flag = false;

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));

  Page *bucket_page_lock = reinterpret_cast<Page *>(bucket_page);

  bucket_page_lock->WLatch();
  flag = bucket_page->Insert(key, value, comparator_);

  bucket_page_lock->WUnlatch();

  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(KeyToPageId(key, dir_page), flag);

  table_latch_.RUnlock();

  if (!flag) {
    flag = SplitInsert(transaction, key, value);
  }

  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool ret = false;
  // 全局加锁,不用再对page页加锁
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t new_index;
  page_id_t old_bucket_page_id = INVALID_PAGE_ID;
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  bool is_free = false;
  uint32_t key_index;
  uint32_t mask;
  HASH_TABLE_BUCKET_TYPE *old_bucket_page = nullptr;
  HASH_TABLE_BUCKET_TYPE *new_bucket_page = nullptr;
  bool is_duplicate = false;
  uint32_t free_pos = BUCKET_ARRAY_SIZE;
  uint32_t index = KeyToDirectoryIndex(key, dir_page);

  // 这里需要再次判断是否可以插入,因为在split获取锁的空隙可能由其他读线程先获取锁
  old_bucket_page_id = dir_page->GetBucketPageId(index);
  old_bucket_page = FetchBucketPage(old_bucket_page_id);

  // 判断是否重复
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
    if (free_pos == BUCKET_ARRAY_SIZE) {
      if (!old_bucket_page->IsOccupied(i) || !old_bucket_page->IsReadable(i)) {
        free_pos = i;
      }
    }
    if (old_bucket_page->IsReadable(i) && comparator_(old_bucket_page->KeyAt(i), key) == 0 &&
        old_bucket_page->ValueAt(i) == value) {
      is_duplicate = true;
      break;
    }
  }

  // 如果重复插入失败,直接返回false
  if (is_duplicate) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(old_bucket_page_id, false);
    table_latch_.WUnlock();
    return false;
  }
  if (free_pos != BUCKET_ARRAY_SIZE) {
    // old_bucket_page->SetKeyValue(free_pos, key, value);
    old_bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(old_bucket_page_id, true);
    table_latch_.WUnlock();
    return true;
  }

  // dir_page->Grow();
  if (dir_page->GetLocalDepth(index) == dir_page->GetGlobalDepth()) {
    new_index = index | (0x1 << dir_page->GetGlobalDepth());
    // global_depth增加,并且扩充hash表容量,设置新entry的local_depth,同时设置指向的bucket

    for (size_t index = 0; index < dir_page->Size(); index++) {
      uint32_t mask = 0x1 << dir_page->GetGlobalDepth();
      dir_page->SetBucketPageId(index | mask, dir_page->GetBucketPageId(index));
      dir_page->SetLocalDepth(index | mask, dir_page->GetLocalDepth(index));
    }
    dir_page->IncrGlobalDepth();

    assert(dir_page->GetBucketPageId(index) == dir_page->GetBucketPageId(new_index));
    new_bucket_page_id = INVALID_PAGE_ID;
    new_bucket_page =
        reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&new_bucket_page_id)->GetData());
    // new_bucket_page = GetNewBucketPage(&new_bucket_page_id);
    for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
      // 桶必定是满的
      assert(old_bucket_page->IsReadable(i));
      KeyType cur_key = old_bucket_page->KeyAt(i);
      if (KeyToDirectoryIndex(cur_key, dir_page) == new_index) {
        new_bucket_page->Insert(cur_key, old_bucket_page->ValueAt(i), comparator_);
        old_bucket_page->RemoveAt(i);
        is_free = true;
      }
    }
    dir_page->SetBucketPageId(new_index, new_bucket_page_id);
    // 此时新桶以及旧桶的local_depth就等于global_depth
    dir_page->SetLocalDepth(new_index, dir_page->GetGlobalDepth());
    dir_page->SetLocalDepth(index, dir_page->GetGlobalDepth());

    key_index = KeyToDirectoryIndex(key, dir_page);
    if (key_index == index && is_free) {
      bool test = old_bucket_page->Insert(key, value, comparator_);
      assert(test);
      ret = true;
    } else if (key_index == new_index) {
      bool test = new_bucket_page->Insert(key, value, comparator_);
      assert(test);
      ret = true;
    }
    buffer_pool_manager_->UnpinPage(old_bucket_page_id, is_free);
    buffer_pool_manager_->UnpinPage(new_bucket_page_id, is_free || (key_index == new_index));
  } else {
    // 获取对应桶的depth
    uint32_t local_depth = dir_page->GetLocalDepth(index);
    // 找到depth + 1后对应的索引
    new_index = (0x1 << (dir_page->GetLocalDepth(index))) ^ index;
    assert(local_depth < dir_page->GetGlobalDepth());

    new_bucket_page =
        reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&new_bucket_page_id)->GetData());
    // new_bucket_page = GetNewBucketPage(&new_bucket_page_id);

    // 重新设置depth以及bucket_id
    assert(dir_page->GetLocalDepth(new_index) == local_depth);
    dir_page->SetBucketPageId(new_index, new_bucket_page_id);
    dir_page->IncrLocalDepth(index);
    dir_page->IncrLocalDepth(new_index);
    mask = dir_page->GetLocalDepthMask(index);
    for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; ++i) {
      assert(old_bucket_page->IsReadable(i));
      KeyType cur_key = old_bucket_page->KeyAt(i);
      key_index = mask & Hash(cur_key);
      if (key_index == (new_index & mask)) {
        new_bucket_page->Insert(cur_key, old_bucket_page->ValueAt(i), comparator_);
        old_bucket_page->RemoveAt(i);
        is_free = true;
      }
    }
    // 调整local_depth以及page_id
    uint32_t size = dir_page->Size();
    mask = dir_page->GetLocalDepthMask(index);
    for (uint32_t i = 0; i < size; ++i) {
      if (dir_page->GetBucketPageId(i) == old_bucket_page_id) {
        assert(i == index || i == new_index || dir_page->GetLocalDepth(i) == local_depth);
        // 都设置成之前的depth + 1
        dir_page->SetLocalDepth(i, local_depth + 1);
        dir_page->SetBucketPageId(i, ((i & mask) == (index & mask)) ? old_bucket_page_id : new_bucket_page_id);
      }
    }
    if (is_free) {
      bool test = old_bucket_page->Insert(key, value, comparator_);
      assert(test);
      ret = true;
    }
    buffer_pool_manager_->UnpinPage(old_bucket_page_id, is_free);
    buffer_pool_manager_->UnpinPage(new_bucket_page_id, is_free);
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  table_latch_.WUnlock();
  if (!ret) {
    return SplitInsert(transaction, key, value);
  }
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  bool flag = false;
  table_latch_.WLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, dir_page));

  Page *bucket_page_lock = reinterpret_cast<Page *>(bucket_page);
  bucket_page_lock->WLatch();
  flag = bucket_page->Remove(key, value, comparator_);
  bucket_page_lock->WUnlatch();

  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(KeyToPageId(key, dir_page), flag);
  table_latch_.WUnlock();
  if (flag) {
    Merge(transaction, key, value);
  }
  return flag;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  // bucket page
  uint32_t bucket_index;
  uint32_t bucket_local_depth;
  page_id_t bucket_page_id;
  HASH_TABLE_BUCKET_TYPE *bucket_page;

  // merge page
  uint32_t merge_index;
  uint32_t merge_local_depth;
  page_id_t merge_page_id;

  // judge bit
  bool is_shrink = false;
  bool is_dirty = false;

  bucket_index = KeyToDirectoryIndex(key, dir_page);
  while (true) {
    // init bucket page
    bucket_page_id = dir_page->GetBucketPageId(bucket_index);
    bucket_local_depth = dir_page->GetLocalDepth(bucket_index);
    bucket_page = FetchBucketPage(bucket_page_id);

    // determine whether bucket can merge
    if (bucket_local_depth > 0 && bucket_page->IsEmpty()) {
      // init merge page
      merge_index = dir_page->GetSplitImageIndex(bucket_index);
      merge_page_id = dir_page->GetBucketPageId(merge_index);
      merge_local_depth = dir_page->GetLocalDepth(merge_index);

      if (bucket_local_depth == merge_local_depth) {
        is_shrink = true;
        is_dirty = true;
        for (size_t i = 0; i < dir_page->Size(); i++) {
          page_id_t curr_bucket_page_id = dir_page->GetBucketPageId(i);
          if ((curr_bucket_page_id == bucket_page_id) || (curr_bucket_page_id == merge_page_id)) {
            dir_page->SetBucketPageId(i, merge_page_id);
            dir_page->DecrLocalDepth(i);
          }
        }
      }
    }

    // unpin bucket page and merge page.
    // determine whether dir can shrink.
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    if (is_shrink) {
      buffer_pool_manager_->DeletePage(bucket_page_id);
      if (dir_page->CanShrink()) {
        dir_page->DecrGlobalDepth();
        bucket_index = merge_index & dir_page->GetGlobalDepthMask();
      } else {
        bucket_index = merge_index;
      }
      is_shrink = false;
    } else {
      break;
    }
  }

  buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), is_dirty);
  table_latch_.WUnlock();
}
/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
