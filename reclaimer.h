#ifndef RECLAIMER_H
#define RECLAIMER_H

#include <atomic>
#include <cassert>
#include <functional>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// A coefficient that used to calcuate the max number
// of reclaim node in reclaim list.
const int kCoefficient = 4 + 1 / 4;

struct HazardPointer {
  HazardPointer() : ptr(nullptr), next(nullptr) {}
  ~HazardPointer() {}

  HazardPointer(const HazardPointer& other) = delete;
  HazardPointer(HazardPointer&& other) = delete;
  HazardPointer& operator=(const HazardPointer& other) = delete;
  HazardPointer& operator=(HazardPointer&& other) = delete;

  std::atomic_flag flag;
  // We must use atomic pointer to ensure the modification
  // is visible to other threads.
  std::atomic<void*> ptr;
  std::atomic<HazardPointer*> next;
};

struct HazardPointerList {
  HazardPointerList() : head(new HazardPointer()) {}
  ~HazardPointerList() {
    // HazardPointerList destruct when program exit.
    HazardPointer* p = head.load(std::memory_order_acquire);
    while (p) {
      HazardPointer* temp = p;
      p = p->next;
      delete temp;
    }
  }

  size_t get_size() const { return size.load(std::memory_order_consume); }

  std::atomic<HazardPointer*> head;
  std::atomic<size_t> size;
};

class Reclaimer {
 public:
  static Reclaimer& GetInstance(HazardPointerList& hazard_pointer_list) {
    // Each thread has its own reclaimer.
    thread_local static Reclaimer reclaimer(hazard_pointer_list);
    return reclaimer;
  }

  Reclaimer(const Reclaimer&) = delete;
  Reclaimer(Reclaimer&&) = delete;
  Reclaimer& operator=(const Reclaimer&) = delete;
  Reclaimer& operator=(Reclaimer&&) = delete;

  // Use an hazard pointer at the index of hazard_pointers_ array to mark a ptr
  // as an hazard pointer pointer.
  void MarkHazard(int index, void* const ptr) {
    TryAcquireHazardPointer(index);
    hazard_pointers_[index]->ptr.store(ptr, std::memory_order_release);
  }

  // Get ptr that marked as hazard at the index of hazard_pointers_ array.
  void* GetHazardPtr(int index) {
    TryAcquireHazardPointer(index);
    return hazard_pointers_[index]->ptr.load(std::memory_order_consume);
  }

  // If ptr is hazard then reclaim it later.
  void ReclaimLater(void* const ptr, std::function<void(void*)>&& func) {
    ReclaimNode* new_node = reclaim_pool_.Pop();
    new_node->ptr = ptr;
    new_node->delete_func = std::move(func);
    reclaim_map_.insert(std::make_pair(ptr, new_node));
  }

  // Try to reclaim all no hazard pointers.
  void ReclaimNoHazardPointer() {
    if (reclaim_map_.size() < kCoefficient * hazard_pointer_list_.get_size()) {
      return;
    }

    // Used to speed up the inspection of the ptr.
    std::unordered_set<void*> not_allow_delete_set;
    std::atomic<HazardPointer*>& head = hazard_pointer_list_.head;
    HazardPointer* p = head.load(std::memory_order_acquire);
    do {
      void* const ptr = p->ptr.load(std::memory_order_consume);
      if (nullptr != ptr) {
        not_allow_delete_set.insert(ptr);
      }
      p = p->next.load(std::memory_order_acquire);
    } while (p);

    for (auto it = reclaim_map_.begin(); it != reclaim_map_.end();) {
      if (not_allow_delete_set.find(it->first) == not_allow_delete_set.end()) {
        ReclaimNode* node = it->second;
        node->delete_func(node->ptr);
        reclaim_pool_.Push(node);
        it = reclaim_map_.erase(it);
      } else {
        ++it;
      }
    }
  }

 private:
  Reclaimer(HazardPointerList& hazard_pointer_list)
      : hazard_pointer_list_(hazard_pointer_list) {}

  ~Reclaimer() {
    // The Reclaimer destruct when the thread exit

    // 1.Hand over the hazard pointer
    for (int i = 0; i < hazard_pointers_.size(); ++i) {
      // If assert fired, you should make sure no pointer is marked as hazard
      // before thread exit
      assert(nullptr ==
             hazard_pointers_[i]->ptr.load(std::memory_order_consume));
      hazard_pointers_[i]->flag.clear();
    }

    // 2.Make sure reclaim all no hazard pointers
    for (auto it = reclaim_map_.begin(); it != reclaim_map_.end();) {
      // Wait until pointer is no hazard
      while (Hazard(it->first)) {
        std::this_thread::yield();
      }

      ReclaimNode* node = it->second;
      node->delete_func(node->ptr);
      delete node;
      it = reclaim_map_.erase(it);
    }
  }

  // Check if the ptr is hazard.
  bool Hazard(void* const ptr) {
    std::atomic<HazardPointer*>& head = hazard_pointer_list_.head;
    HazardPointer* p = head.load(std::memory_order_acquire);
    do {
      if (p->ptr.load(std::memory_order_consume) == ptr) {
        return true;
      }
      p = p->next.load(std::memory_order_acquire);
    } while (p != nullptr);

    return false;
  }

  void TryAcquireHazardPointer(int index) {
    assert(index <= hazard_pointers_.size());
    if (index < hazard_pointers_.size()) {
      return;
    }

    std::atomic<HazardPointer*>& head = hazard_pointer_list_.head;
    HazardPointer* p = head.load(std::memory_order_acquire);
    HazardPointer* hp = nullptr;
    do {
      // Try to get the idle hazard pointer.
      if (!p->flag.test_and_set()) {
        hp = p;
        break;
      }
      p = p->next.load(std::memory_order_acquire);
    } while (p != nullptr);

    if (nullptr == hp) {
      // No idle hazard pointer, allocate new one.
      HazardPointer* new_head = new HazardPointer();
      new_head->flag.test_and_set();
      hp = new_head;
      hazard_pointer_list_.size.fetch_add(1, std::memory_order_release);
      HazardPointer* old_head = head.load(std::memory_order_acquire);
      do {
        new_head->next = old_head;
      } while (!head.compare_exchange_weak(old_head, new_head,
                                           std::memory_order_release,
                                           std::memory_order_relaxed));
    }
    hazard_pointers_.push_back(hp);
  }

  struct ReclaimNode {
    ReclaimNode() : ptr(nullptr), next(nullptr), delete_func(nullptr) {}
    ~ReclaimNode() {}

    void* ptr;
    ReclaimNode* next;
    std::function<void(void*)> delete_func;
  };

  // Reuse ReclaimNode
  struct ReclaimPool {
    ReclaimPool() : head(new ReclaimNode()) {}
    ~ReclaimPool() {
      ReclaimNode* temp;
      while (head) {
        temp = head;
        head = head->next;
        delete temp;
      }
    }

    void Push(ReclaimNode* node) {
      node->next = head;
      head = node;
    }

    ReclaimNode* Pop() {
      if (nullptr == head->next) {
        return new ReclaimNode();
      }

      ReclaimNode* temp = head;
      head = head->next;
      temp->next = nullptr;
      return temp;
    }

    ReclaimNode* head;
  };

  std::vector<HazardPointer*> hazard_pointers_;
  std::unordered_map<void*, ReclaimNode*> reclaim_map_;
  ReclaimPool reclaim_pool_;
  HazardPointerList& hazard_pointer_list_;
};
#endif  // RECLAIMER_H
