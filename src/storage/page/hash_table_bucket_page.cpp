//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/hash_table_page_defs.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool flag = false;
  for (size_t curr_idx = 0; curr_idx < BUCKET_ARRAY_SIZE; curr_idx++) {
    if (IsReadable(curr_idx) && cmp(KeyAt(curr_idx), key) == 0) {
      flag = true;
      result->emplace_back(ValueAt(curr_idx));
    }
  }
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  size_t insert_index = BUCKET_ARRAY_SIZE;
  for (size_t curr_idx = 0; curr_idx < BUCKET_ARRAY_SIZE; curr_idx++) {
    if (insert_index == BUCKET_ARRAY_SIZE) {
      if (!IsReadable(curr_idx)) {
        insert_index = curr_idx;
      }
    }
    if (IsReadable(curr_idx) && cmp(array_[curr_idx].first, key) == 0 && array_[curr_idx].second == value) {
      return false;
    }
  }
  if (insert_index == BUCKET_ARRAY_SIZE) {
    return false;
  }

  array_[insert_index].first = key;
  array_[insert_index].second = value;
  SetOccupied(insert_index);
  SetReadable(insert_index);
  return true;
}

// Note that you can equality-test ValueType instances simply using the == operator.
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  bool flag = false;
  for (size_t curr_idx = 0; curr_idx < BUCKET_ARRAY_SIZE; curr_idx++) {
    if (IsReadable(curr_idx) && cmp(KeyAt(curr_idx), key) == 0 && ValueAt(curr_idx) == value) {
      flag = true;
      RemoveAt(curr_idx);
    }
  }
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  // 将readable_相应的位置设置为0
  char &change = readable_[bucket_idx / 8];
  // eg. 10101010 & 11101111
  change &= ~(0x1 << (7 - (bucket_idx % 8)));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  // 返回occupied_相应位置的0/1
  const int bits = static_cast<int>(occupied_[bucket_idx / 8]);
  return (bits & (0x1 << (7 - (bucket_idx % 8)))) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  // 将occupied_相应位置设置为1
  char &change = occupied_[bucket_idx / 8];
  change |= (0x1 << (7 - (bucket_idx % 8)));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  // 返回readable_相应位置的0/1
  const int bits = static_cast<int>(readable_[bucket_idx / 8]);
  return (bits & (0x1 << (7 - (bucket_idx % 8)))) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  // 将readable_相应位置设置为1
  char &change = readable_[bucket_idx / 8];
  change |= (0x1 << (7 - (bucket_idx % 8)));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  // 如果是满的 每一个readable_[i] 都应该是 0x00
  for (size_t i = 0; i < (BUCKET_ARRAY_SIZE - 1) / 8 + 1; i++) {
    if ((readable_[i] & 0xff) != 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  int ans = 0;
  for (size_t curr_idx = 0; curr_idx < BUCKET_ARRAY_SIZE; curr_idx++) {
    ans += IsReadable(curr_idx) ? 1 : 0;
  }
  return ans;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
