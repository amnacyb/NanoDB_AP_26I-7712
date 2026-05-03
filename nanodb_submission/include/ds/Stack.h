#pragma once
#include <cstdlib>
#include <cassert>

// ─── Custom Stack (no STL) ───────────────────────────────────────────────────
// Simple dynamic array stack with amortized O(1) push/pop.
template<typename T>
class Stack {
    T*  data;
    int top_;
    int cap;

    void grow() {
        int newCap = cap * 2;
        T* newData = new T[newCap];
        for (int i = 0; i < top_; i++) newData[i] = data[i];
        delete[] data;
        data = newData;
        cap  = newCap;
    }
public:
    explicit Stack(int initCap = 64)
        : data(new T[initCap]), top_(0), cap(initCap) {}

    ~Stack() { delete[] data; }

    // No copy (avoid accidental copies)
    Stack(const Stack&) = delete;
    Stack& operator=(const Stack&) = delete;

    void push(const T& val) {
        if (top_ == cap) grow();
        data[top_++] = val;
    }

    T pop() {
        assert(top_ > 0 && "Stack underflow");
        return data[--top_];
    }

    T& peek() {
        assert(top_ > 0 && "Stack empty");
        return data[top_ - 1];
    }

    const T& peek() const {
        assert(top_ > 0 && "Stack empty");
        return data[top_ - 1];
    }

    bool isEmpty() const { return top_ == 0; }
    int  size()    const { return top_; }
    void clear()         { top_ = 0; }
};
