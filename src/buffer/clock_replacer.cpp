//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include <climits>
#include "common/logger.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  capacity_ = num_pages;
  pin_size_ = 0;
  clock_size_ = 0;
}

ClockReplacer::~ClockReplacer() = default;

/**
 * case empty => false
 * case all pin => false
 * case all refer=1 => min frame_id
 * other: from current_ to first node which refer=0
 */
auto ClockReplacer::Victim(frame_id_t *frame_id) -> bool {
  if (head_ == nullptr) {
    // LOG_DEBUG("Victim, head_ ==nullptr");
    return false;
  }

  size_t cnt = 0;
  ListNode *target = nullptr;
  ListNode *pre = nullptr;
  ListNode *min_pre = nullptr;
  ListNode *min_frame = nullptr;
  frame_id_t min_frame_id = INT_MAX;

  // 从current开始, 查找 victim节点
  while (cnt < clock_size_) {
    if (current_ == nullptr) {
      break;
    }

    if (current_->pin_) {
      // just skip
      pre = current_;
      current_ = current_->next_;
      cnt++;
      continue;
    }

    // 如果该节点 !refer
    if (!current_->refer_) {
      target = current_;
      break;  // found
    }

    // not pin and refer
    current_->refer_ = false;
    if (current_->frame_id_ < min_frame_id) {
      min_frame_id = current_->frame_id_;
      min_frame = current_;
      min_pre = pre;
    }

    pre = current_;
    current_ = current_->next_;
    cnt++;
  }

  if (target == nullptr && min_frame != nullptr) {
    target = min_frame;
    pre = min_pre;
  }

  if (target != nullptr) {
    // LOG_DEBUG("Victim, found, target frame_id=%d", target->frame_id_);

    // 处理指针
    if (clock_size_ == 1) {
      // last one
      head_ = nullptr;
      tail_ = nullptr;
      current_ = nullptr;
    } else {
      if (target == head_) {
        head_ = target->next_;
        tail_->next_ = head_;
      } else {
        if (pre == nullptr) {
          pre = head_;
        }
        pre->next_ = target->next_;
      }
    }

    if (current_ == target) {
      current_ = target->next_;
    }

    *frame_id = target->frame_id_;

    // 处理
    target->next_ = nullptr;
    free(target);
    clock_size_--;
    return true;
  }
  // LOG_DEBUG("Victim, not found");

  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  // if empty then add
  if (clock_size_ == 0) {
    return;
  }

  // first search, if not exist then add
  size_t cnt = 0;
  ListNode *node = head_;
  ListNode *target = nullptr;

  while (cnt < clock_size_) {
    if (node == nullptr) {
      break;
    }

    // check this node
    if (node->frame_id_ == frame_id) {
      target = node;
      break;
    }

    // not this node
    node = node->next_;
    cnt++;
  }

  // found
  if (target != nullptr) {
    target->pin_ = true;
    target->refer_ = true;
    pin_size_++;
  }

  // not found, do nothing
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  // if empty then add
  if (clock_size_ == 0) {
    // LOG_DEBUG("empty, add frame_id=%d", frame_id);
    head_ = static_cast<ListNode *>(Create_node(frame_id, false, nullptr));
    head_->next_ = head_;
    current_ = head_;
    return;
  }

  // first search, if not exist then add
  size_t cnt = 0;
  ListNode *node = head_;
  ListNode *target = nullptr;
  ListNode *pre = nullptr;

  while (cnt < clock_size_) {
    if (node == nullptr) {
      break;
    }

    // check this node
    if (node->frame_id_ == frame_id) {
      target = node;
      break;
    }

    // not this node
    pre = node;
    node = node->next_;
    cnt++;
  }

  // found
  if (target != nullptr) {
    // LOG_DEBUG("unpin found frame_id=%d", frame_id);
    if (target->pin_) {
      target->pin_ = false;
      pin_size_--;
    }
    return;
  }

  // not found
  if (clock_size_ < capacity_) {
    // LOG_DEBUG("unpin not found, add frame_id=%d", frame_id);
    // case: not full
    ListNode *new_node = static_cast<ListNode *>(Create_node(frame_id, false, head_));
    pre->next_ = new_node;
    tail_ = new_node;
  } else {
    // LOG_DEBUG("unpin not found, full, frame_id=%d", frame_id);
    // full, we should evacuate one element
    throw "TODO not implement";
  }
}

auto ClockReplacer::Size() -> size_t { return clock_size_ - pin_size_; }

void *ClockReplacer::Create_node(frame_id_t frame_id, bool pin, void *next) {
  ListNode *new_node = static_cast<ListNode *>(malloc(sizeof(ListNode)));
  new_node->pin_ = pin;
  new_node->frame_id_ = frame_id;
  new_node->refer_ = true;
  new_node->next_ = static_cast<ListNode *>(next);

  // increase size
  clock_size_++;

  return new_node;
}

}  // namespace bustub
