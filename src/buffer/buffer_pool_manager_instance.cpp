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

#include "buffer/clock_replacer.h"
#include "common/logger.h"
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
  // replacer_ = new LRUReplacer(pool_size);
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  // Make sure you call DiskManager::WritePage!
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id;
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // found in page_table
    frame_id = it->second;
    Page *page = pages_ + frame_id;
    if (page->IsDirty()) {
      page->WLatch();
      disk_manager_->WritePage(page_id, reinterpret_cast<char *>(page));
      page->is_dirty_ = false;
      page->WUnlatch();
    }
  }

  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  Page *page;
  for (auto &iter : page_table_) {
    page = pages_ + iter.second;
    if (page->IsDirty()) {
      disk_manager_->WritePage(iter.first, reinterpret_cast<char *>(page));
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  *page_id = AllocatePage();
  // LOG_DEBUG("page_id=%d", *page_id);
  frame_id_t frame_id;
  Page *page = nullptr;

  // 2
  if (!free_list_.empty()) {  // have empty frame
    // LOG_DEBUG("have empty frame");
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = pages_ + frame_id;
  } else {
    // LOG_DEBUG("frame full, need victim");
    bool is_found = replacer_->Victim(&frame_id);
    if (is_found) {
      // write old page if dirty
      Page *old_page = pages_ + frame_id;
      int old_page_id = old_page->page_id_;
      if (old_page->IsDirty()) {
        // LOG_DEBUG("write dirty to disk, page_id=%d", old_page_id);
        old_page->WLatch();
        old_page->is_dirty_ = false;
        disk_manager_->WritePage(old_page_id, reinterpret_cast<char *>(old_page));
        old_page->WUnlatch();
      }

      // remove from page_table_
      auto it = page_table_.find(old_page_id);
      if (it != page_table_.end()) {
        page_table_.erase(it);
      } else {
        // LOG_DEBUG("error: should be found %d in page_table_, actually not found", old_page_id);
      }

      page = pages_ + frame_id;
    }
  }

  if (page != nullptr) {
    disk_manager_->ReadPage(*page_id, reinterpret_cast<char *>(page));

    page->pin_count_ = 1;
    page->is_dirty_ = false;
    page->page_id_ = *page_id;
    replacer_->Unpin(frame_id);  // 首先添加, 在pin
    replacer_->Pin(frame_id);

    // page->page_id_ = INVALID_PAGE_ID;
    std::pair<page_id_t, frame_id_t> pair(*page_id, frame_id);
    page_table_.insert(page_table_.begin(), pair);
  } else {
    // LOG_DEBUG("error: no frame for new page, all pinned");
  }

  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  // LOG_DEBUG("page_id=%d", page_id);
  frame_id_t frame_id;

  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // LOG_DEBUG("page %d in page_table_", page_id);
    frame_id = it->second;
    replacer_->Pin(frame_id);
    return pages_ + frame_id;
  }

  Page *page = nullptr;  // if found proper page, fill this page for page_id
  if (!free_list_.empty()) {
    // LOG_DEBUG("page %d not in page_table_, has free_list_", page_id);
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = pages_ + frame_id;  // page from free_list_
  } else {
    bool is_found = replacer_->Victim(&frame_id);
    if (is_found) {
      // LOG_DEBUG("page %d not in page_table_, free_list_ empty, need victim, found frame_id=%d", page_id, frame_id);
      page = pages_ + frame_id;
    }
    // else: no free_list_, no victim
  }
  if (page != nullptr) {
    if (page->IsDirty()) {
      page->WLatch();
      page->is_dirty_ = false;
      disk_manager_->WritePage(page->page_id_, reinterpret_cast<char *>(page));
      page->WUnlatch();
    }

    // remove from page_table_
    auto it2 = page_table_.find(page->page_id_);
    if (it2 != page_table_.end()) {
      page_table_.erase(it2);
    } else {
      // LOG_DEBUG("error: should be found %d in page_table_, actually not found", page->page_id_);
    }

    page->WLatch();
    // read from disk, and unpin it
    disk_manager_->ReadPage(page_id, reinterpret_cast<char *>(page));
    page->page_id_ = page_id;
    page->is_dirty_ = false;
    page->pin_count_ = 1;
    page->WUnlatch();

    replacer_->Unpin(frame_id);  // error
    replacer_->Pin(frame_id);

    // add to page_table_
    std::pair<page_id_t, frame_id_t> pair(page_id, frame_id);
    page_table_.insert(page_table_.begin(), pair);
  } else {
    // LOG_DEBUG("page %d not in page_table_, free_list_ empty, need victim, not found => give up", page_id);
  }

  return page;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  if (page_id == INVALID_PAGE_ID) {  // not exist
    return true;
  }

  frame_id_t frame_id;
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {  // not exist
    return true;
  }

  frame_id = it->second;
  Page *page = pages_ + frame_id;
  if (page->pin_count_ > 0) {  // non-zero pin-count
    return false;
  }

  // can be deleted, do delete
  DeallocatePage(page_id);
  page->page_id_ = INVALID_PAGE_ID;
  // delete from page_table_
  page_table_.erase(it);
  // add to free_list_
  free_list_.push_back(frame_id);

  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // not found in page_table
    return true;
  }

  // found in page_table
  frame_id_t frame_id = it->second;
  Page *page = pages_ + frame_id;
  page->is_dirty_ = is_dirty;

  if (page->pin_count_ > 0) {
    replacer_->Unpin(frame_id);
    page->pin_count_ = 0;
    return true;
  }

  return false;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
