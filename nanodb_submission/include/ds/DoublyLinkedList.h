#pragma once
#include <cassert>
#include <cstddef>

// ─── Doubly Linked List Node ─────────────────────────────────────────────────
template<typename T>
struct DLLNode {
    T          data;
    DLLNode<T>* prev;
    DLLNode<T>* next;

    explicit DLLNode(const T& val)
        : data(val), prev(nullptr), next(nullptr) {}
};

// ─── Doubly Linked List ──────────────────────────────────────────────────────
// Head = Most Recently Used, Tail = Least Recently Used.
// All key operations are O(1) given a node pointer.
template<typename T>
class DoublyLinkedList {
    DLLNode<T>* head_;
    DLLNode<T>* tail_;
    int         size_;

public:
    DoublyLinkedList() : head_(nullptr), tail_(nullptr), size_(0) {}

    ~DoublyLinkedList() {
        DLLNode<T>* cur = head_;
        while (cur) {
            DLLNode<T>* nxt = cur->next;
            delete cur;
            cur = nxt;
        }
    }

    // Add node at front (MRU end), returns pointer to new node.
    DLLNode<T>* addFront(const T& val) {
        DLLNode<T>* node = new DLLNode<T>(val);
        node->next = head_;
        node->prev = nullptr;
        if (head_) head_->prev = node;
        head_ = node;
        if (!tail_) tail_ = node;
        size_++;
        return node;
    }

    // Remove node in O(1) given pointer (no search needed).
    void removeNode(DLLNode<T>* node) {
        assert(node && "removeNode: null node");
        if (node->prev) node->prev->next = node->next;
        else            head_            = node->next;
        if (node->next) node->next->prev = node->prev;
        else            tail_            = node->prev;
        node->prev = node->next = nullptr;
        delete node;
        size_--;
    }

    // Move an existing node to the front (mark as MRU) in O(1).
    void moveToFront(DLLNode<T>* node) {
        if (node == head_) return;           // already MRU
        // Unlink
        if (node->prev) node->prev->next = node->next;
        if (node->next) node->next->prev = node->prev;
        if (node == tail_) tail_ = node->prev;
        // Relink at front
        node->prev = nullptr;
        node->next = head_;
        if (head_) head_->prev = node;
        head_ = node;
        if (!tail_) tail_ = node;
    }

    // Remove and return data of LRU (tail) node in O(1).
    T removeLast() {
        assert(tail_ && "removeLast: list is empty");
        T val = tail_->data;
        DLLNode<T>* old = tail_;
        tail_ = tail_->prev;
        if (tail_) tail_->next = nullptr;
        else       head_       = nullptr;
        delete old;
        size_--;
        return val;
    }

    DLLNode<T>* front() const { return head_; }
    DLLNode<T>* back()  const { return tail_; }
    int          size()  const { return size_; }
    bool         empty() const { return size_ == 0; }
};
