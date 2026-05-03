#pragma once
#include "../common.h"

// ─── Query Job ───────────────────────────────────────────────────────────────
struct QueryJob {
    char query[MAX_QUERY_LEN];
    int  priority;   // higher = more urgent
    int  seqNum;     // tie-breaker (lower = submitted first)

    QueryJob() : priority(0), seqNum(0) { query[0] = '\0'; }
    QueryJob(const char* q, int p, int seq) : priority(p), seqNum(seq) {
        safe_strcpy(query, q, MAX_QUERY_LEN);
    }

    // Higher priority first; if tied, lower seqNum first (FIFO for same priority)
    bool operator>(const QueryJob& o) const {
        if (priority != o.priority) return priority > o.priority;
        return seqNum < o.seqNum;
    }
};

// ─── Priority Queue (Max-Heap) ───────────────────────────────────────────────
// Built on a raw array. O(log N) insert & extract-max.
class PriorityQueue {
    QueryJob* heap;
    int       size_;
    int       cap;

    void grow() {
        int nc = cap * 2;
        QueryJob* nh = new QueryJob[nc];
        for (int i = 0; i < size_; i++) nh[i] = heap[i];
        delete[] heap;
        heap = nh;
        cap  = nc;
    }

    void heapifyUp(int i) {
        while (i > 0) {
            int parent = (i - 1) / 2;
            if (heap[i] > heap[parent]) {
                QueryJob tmp  = heap[i];
                heap[i]       = heap[parent];
                heap[parent]  = tmp;
                i = parent;
            } else break;
        }
    }

    void heapifyDown(int i) {
        while (true) {
            int largest = i;
            int l = 2*i + 1, r = 2*i + 2;
            if (l < size_ && heap[l] > heap[largest]) largest = l;
            if (r < size_ && heap[r] > heap[largest]) largest = r;
            if (largest == i) break;
            QueryJob tmp = heap[i]; heap[i] = heap[largest]; heap[largest] = tmp;
            i = largest;
        }
    }

public:
    explicit PriorityQueue(int initCap = 128)
        : heap(new QueryJob[initCap]), size_(0), cap(initCap) {}

    ~PriorityQueue() { delete[] heap; }

    void insert(const QueryJob& job) {
        if (size_ == cap) grow();
        heap[size_++] = job;
        heapifyUp(size_ - 1);
    }

    QueryJob extractMax() {
        assert(size_ > 0 && "PriorityQueue is empty");
        QueryJob top = heap[0];
        heap[0] = heap[--size_];
        if (size_ > 0) heapifyDown(0);
        return top;
    }

    const QueryJob& peekMax() const {
        assert(size_ > 0);
        return heap[0];
    }

    bool isEmpty() const { return size_ == 0; }
    int  size()    const { return size_; }
};
