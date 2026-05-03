#pragma once
#include "../common.h"

// ─── Generic HashMap with string keys & chaining ─────────────────────────────
// Achieves amortized O(1) insert / lookup / delete.
// Collision resolution: separate chaining via linked list nodes.
template<typename V>
class HashMap {
    struct Entry {
        char    key[MAX_TABLE_NAME];
        V       value;
        Entry*  next;
        explicit Entry(const char* k, const V& v)
            : value(v), next(nullptr) { safe_strcpy(key, k, MAX_TABLE_NAME); }
    };

    Entry** buckets;
    int     numBuckets;
    int     count;

    // djb2 hash
    uint32_t hashKey(const char* k) const {
        uint32_t h = 5381;
        while (*k) h = ((h << 5) + h) ^ (unsigned char)(*k++);
        return h % (uint32_t)numBuckets;
    }

public:
    explicit HashMap(int nb = HASH_BUCKETS)
        : numBuckets(nb), count(0)
    {
        buckets = new Entry*[numBuckets];
        for (int i = 0; i < numBuckets; i++) buckets[i] = nullptr;
    }

    ~HashMap() {
        clear();
        delete[] buckets;
    }

    void put(const char* key, const V& val) {
        uint32_t idx = hashKey(key);
        Entry* cur = buckets[idx];
        while (cur) {
            if (strcmp(cur->key, key) == 0) { cur->value = val; return; }
            cur = cur->next;
        }
        Entry* e    = new Entry(key, val);
        e->next     = buckets[idx];
        buckets[idx] = e;
        count++;
    }

    // Returns pointer to value or nullptr if not found — O(1) amortized.
    V* get(const char* key) {
        uint32_t idx = hashKey(key);
        Entry* cur = buckets[idx];
        while (cur) {
            if (strcmp(cur->key, key) == 0) return &cur->value;
            cur = cur->next;
        }
        return nullptr;
    }

    bool contains(const char* key) const {
        uint32_t idx = hashKey(key);
        Entry* cur = buckets[idx];
        while (cur) {
            if (strcmp(cur->key, key) == 0) return true;
            cur = cur->next;
        }
        return false;
    }

    bool remove(const char* key) {
        uint32_t idx = hashKey(key);
        Entry*  cur  = buckets[idx];
        Entry*  prev = nullptr;
        while (cur) {
            if (strcmp(cur->key, key) == 0) {
                if (prev) prev->next = cur->next;
                else      buckets[idx] = cur->next;
                delete cur;
                count--;
                return true;
            }
            prev = cur;
            cur  = cur->next;
        }
        return false;
    }

    void clear() {
        for (int i = 0; i < numBuckets; i++) {
            Entry* cur = buckets[i];
            while (cur) { Entry* nxt = cur->next; delete cur; cur = nxt; }
            buckets[i] = nullptr;
        }
        count = 0;
    }

    int  size()  const { return count; }
    bool empty() const { return count == 0; }

    // Iterate over all entries (for debug/list)
    template<typename Fn>
    void forEach(Fn fn) const {
        for (int i = 0; i < numBuckets; i++) {
            Entry* cur = buckets[i];
            while (cur) { fn(cur->key, cur->value); cur = cur->next; }
        }
    }
};
