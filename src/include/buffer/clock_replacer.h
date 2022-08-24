//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  // 查找下一个 victim
  auto Victim(frame_id_t *frame_id) -> bool override;

  // 该 frame_id必须存在
  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

  void *Create_node(frame_id_t frame_id, bool pin, void *next);

 private:
  // TODO(student): implement me!
  struct ListNode {
    bool pin_;
    bool refer_;
    frame_id_t frame_id_;
    ListNode *next_;
  };

  size_t capacity_;
  size_t clock_size_;
  size_t pin_size_;

  ListNode *head_;
  ListNode *tail_;
  ListNode *current_;

};

}  // namespace bustub
