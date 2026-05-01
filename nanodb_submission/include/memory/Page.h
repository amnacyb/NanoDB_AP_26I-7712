#pragma once
#include "../common.h"

// ─── Page Header (64 bytes) ──────────────────────────────────────────────────
struct PageHeader {
    uint32_t page_id;          // 4
    uint32_t table_id;         // 4
    uint16_t num_records;      // 2
    uint16_t record_size;      // 2
    uint8_t  is_dirty;         // 1
    char     table_name[32];   // 32
    uint8_t  padding[19];      // 19  → total = 64 bytes
};
static_assert(sizeof(PageHeader) == 64, "PageHeader must be 64 bytes");

// ─── Page ────────────────────────────────────────────────────────────────────
// A raw 4096-byte page. Header at offset 0, records start at PAGE_HEADER_SIZE.
class Page {
    char data_[PAGE_SIZE];    // raw storage (includes header)

public:
    Page() { clear(); }

    void clear() {
        for (int i = 0; i < PAGE_SIZE; i++) data_[i] = 0;
    }

    PageHeader* header() {
        return reinterpret_cast<PageHeader*>(data_);
    }
    const PageHeader* header() const {
        return reinterpret_cast<const PageHeader*>(data_);
    }

    // Pointer to the start of the record area
    char* recordArea() { return data_ + PAGE_HEADER_SIZE; }
    const char* recordArea() const { return data_ + PAGE_HEADER_SIZE; }

    // Pointer to slot i's record
    char* getSlot(int i) {
        return recordArea() + (long)i * header()->record_size;
    }
    const char* getSlot(int i) const {
        return recordArea() + (long)i * header()->record_size;
    }

    int maxRecords() const {
        if (header()->record_size == 0) return 0;
        return PAGE_DATA_SIZE / header()->record_size;
    }

    bool isFull() const {
        return header()->num_records >= (uint16_t)maxRecords();
    }

    bool hasSpace() const { return !isFull(); }

    // Returns slot index of newly inserted record, or -1 if full.
    int insertRecord(const char* rec, uint16_t recSize) {
        if (isFull()) return -1;
        int slot = header()->num_records;
        char* dst = getSlot(slot);
        for (int i = 0; i < recSize; i++) dst[i] = rec[i];
        header()->num_records++;
        header()->is_dirty = 1;
        return slot;
    }

    // Overwrite an existing slot (for UPDATE).
    bool updateRecord(int slot, const char* rec, uint16_t recSize) {
        if (slot < 0 || slot >= header()->num_records) return false;
        char* dst = getSlot(slot);
        for (int i = 0; i < recSize; i++) dst[i] = rec[i];
        header()->is_dirty = 1;
        return true;
    }

    bool isDirty() const { return header()->is_dirty != 0; }
    void setDirty(bool d) { header()->is_dirty = d ? 1 : 0; }

    // Write entire 4096-byte page to file at correct offset
    void writeToDisk(FILE* f, uint32_t page_id) const {
        long offset = (long)page_id * PAGE_SIZE;
        fseek(f, offset, SEEK_SET);
        fwrite(data_, 1, PAGE_SIZE, f);
        fflush(f);
    }

    // Read entire 4096-byte page from file
    bool readFromDisk(FILE* f, uint32_t page_id) {
        long offset = (long)page_id * PAGE_SIZE;
        fseek(f, offset, SEEK_SET);
        size_t r = fread(data_, 1, PAGE_SIZE, f);
        return r == PAGE_SIZE;
    }

    // Raw access for serialization
    char*       raw()       { return data_; }
    const char* raw() const { return data_; }
};
