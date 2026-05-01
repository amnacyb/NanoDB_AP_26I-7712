#pragma once
#include "../common.h"
#include "Page.h"
#include "DiskManager.h"
#include "../Logger.h"
#include "../ds/DoublyLinkedList.h"

// ─── Frame Descriptor ─────────────────────────────────────────────────────────
struct Frame {
    Page  page;
    bool  occupied;
    uint32_t page_id;
    char  table_name[MAX_TABLE_NAME];

    Frame() : occupied(false), page_id(0) { table_name[0] = '\0'; }
};

// ─── Buffer Pool ──────────────────────────────────────────────────────────────
// Fixed-size array of Page frames.
// LRU eviction uses a Doubly Linked List + hash map for O(1) operations.
// The buffer pool is the gateway between all in-memory operations and disk.
class BufferPool {
    Frame*      frames;       // raw array of frames (no std::vector!)
    int         poolSize;
    int         numOccupied;

    // LRU structure:
    // dll_ stores frame indices, head = MRU, tail = LRU
    DoublyLinkedList<int>     dll_;
    // Map (table_name + "_" + page_id) string key -> DLLNode pointer
    // We need a map from frameIdx -> DLLNode* for O(1) moveToFront.
    DLLNode<int>*             frameToNode[DEFAULT_POOL_SIZE + 1];

    // Simple hash map: key = hash(table, page_id) -> frameIdx
    // Use open addressing for the pool hash map
    static const int POOL_HASH = 512;
    struct PoolEntry { char tbl[MAX_TABLE_NAME]; uint32_t pg; int frame; bool used; };
    PoolEntry poolMap[POOL_HASH + 1];

    DiskManager* disk;
    Logger*      logger;

    uint64_t evictionCount_;

    // Hash for pool map
    int poolHash(const char* tbl, uint32_t pg) const {
        uint32_t h = 5381;
        for (const char* c = tbl; *c; c++) h = ((h << 5) + h) ^ (unsigned char)(*c);
        h = ((h << 5) + h) ^ pg;
        return (int)(h % POOL_HASH);
    }

    int findFrame(const char* tbl, uint32_t pg) const {
        int idx = poolHash(tbl, pg);
        for (int i = 0; i < POOL_HASH; i++) {
            int slot = (idx + i) % POOL_HASH;
            if (!poolMap[slot].used) return -1;
            if (poolMap[slot].pg == pg && strcmp(poolMap[slot].tbl, tbl) == 0)
                return poolMap[slot].frame;
        }
        return -1;
    }

    void insertPoolMap(const char* tbl, uint32_t pg, int frame) {
        int idx = poolHash(tbl, pg);
        for (int i = 0; i < POOL_HASH; i++) {
            int slot = (idx + i) % POOL_HASH;
            if (!poolMap[slot].used) {
                poolMap[slot].used = true;
                safe_strcpy(poolMap[slot].tbl, tbl, MAX_TABLE_NAME);
                poolMap[slot].pg    = pg;
                poolMap[slot].frame = frame;
                return;
            }
        }
    }

    void removePoolMap(const char* tbl, uint32_t pg) {
        int idx = poolHash(tbl, pg);
        for (int i = 0; i < POOL_HASH; i++) {
            int slot = (idx + i) % POOL_HASH;
            if (!poolMap[slot].used) return;
            if (poolMap[slot].pg == pg && strcmp(poolMap[slot].tbl, tbl) == 0) {
                poolMap[slot].used = false;
                // Rehash subsequent entries to fill gap (Robin Hood style)
                for (int j = 1; j < POOL_HASH; j++) {
                    int nxt = (slot + j) % POOL_HASH;
                    if (!poolMap[nxt].used) break;
                    PoolEntry tmp = poolMap[nxt];
                    poolMap[nxt].used = false;
                    insertPoolMap(tmp.tbl, tmp.pg, tmp.frame);
                }
                return;
            }
        }
    }

    // Evict LRU frame, return its index. Returns -1 if no eviction possible.
    int evict() {
        if (dll_.empty()) return -1;
        int fid = dll_.removeLast();
        Frame& f = frames[fid];
        if (f.page.isDirty()) {
            disk->writePage(f.table_name, f.page.header()->page_id, &f.page);
            logger->log("Page %u evicted via LRU from pool frame %d, written to disk (table: %s)",
                        f.page.header()->page_id, fid, f.table_name);
        } else {
            logger->log("Page %u evicted via LRU from pool frame %d (clean, no write) (table: %s)",
                        f.page.header()->page_id, fid, f.table_name);
        }
        removePoolMap(f.table_name, f.page.header()->page_id);
        f.occupied = false;
        frameToNode[fid] = nullptr;
        numOccupied--;
        evictionCount_++;
        return fid;
    }

    // Find or allocate a free frame.
    int allocFrame() {
        for (int i = 0; i < poolSize; i++)
            if (!frames[i].occupied) return i;
        return evict();  // pool full → evict LRU
    }

public:
    BufferPool(int size, DiskManager* d, Logger* lg)
        : frames(new Frame[size]), poolSize(size),
          numOccupied(0), disk(d), logger(lg), evictionCount_(0)
    {
        for (int i = 0; i <= DEFAULT_POOL_SIZE; i++) frameToNode[i] = nullptr;
        for (int i = 0; i <= POOL_HASH; i++) poolMap[i].used = false;
        logger->log("BufferPool initialized: %d frames (%d KB)",
                    size, (int)((long)size * PAGE_SIZE / 1024));
    }

    ~BufferPool() {
        flushAll();
        delete[] frames;
    }

    // Get a page; loads from disk if not in pool (transparent to caller).
    Page* getPage(const char* tbl, uint32_t pg) {
        int fid = findFrame(tbl, pg);
        if (fid >= 0) {
            // Cache hit → move to MRU
            dll_.moveToFront(frameToNode[fid]);
            return &frames[fid].page;
        }
        // Cache miss → load from disk
        fid = allocFrame();
        Frame& f = frames[fid];
        f.occupied = true;
        safe_strcpy(f.table_name, tbl, MAX_TABLE_NAME);
        f.page.clear();
        disk->readPage(tbl, pg, &f.page);
        insertPoolMap(tbl, pg, fid);
        DLLNode<int>* node = dll_.addFront(fid);
        frameToNode[fid] = node;
        numOccupied++;
        return &frames[fid].page;
    }

    // Retrieve a page that was previously loaded (must already be in pool).
    // Returns nullptr if not found.
    Page* getIfCached(const char* tbl, uint32_t pg) {
        int fid = findFrame(tbl, pg);
        if (fid < 0) return nullptr;
        dll_.moveToFront(frameToNode[fid]);
        return &frames[fid].page;
    }

    void markDirty(const char* tbl, uint32_t pg) {
        int fid = findFrame(tbl, pg);
        if (fid >= 0) frames[fid].page.setDirty(true);
    }

    // Write all dirty pages to disk without evicting them.
    void flushAll() {
        for (int i = 0; i < poolSize; i++) {
            if (frames[i].occupied && frames[i].page.isDirty()) {
                disk->writePage(frames[i].table_name,
                                frames[i].page.header()->page_id, &frames[i].page);
                logger->log("Flushed dirty page %u (table: %s) to disk",
                            frames[i].page.header()->page_id, frames[i].table_name);
            }
        }
    }

    uint64_t getEvictionCount() const { return evictionCount_; }
    int      getPoolSize()      const { return poolSize; }
    int      getOccupied()      const { return numOccupied; }

    void resetEvictionCount() { evictionCount_ = 0; }
};
