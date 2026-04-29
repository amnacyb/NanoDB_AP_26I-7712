#pragma once
#include "../common.h"
#include "../memory/BufferPool.h"
#include "../memory/DiskManager.h"
#include "../ds/AVLTree.h"
#include "Row.h"
#include "../Logger.h"
#include <cstdio>

// ─── Table ────────────────────────────────────────────────────────────────────
// Manages storage, retrieval, and indexing of rows for a single table.
// Rows are stored in binary pages via the BufferPool.
// A primary AVL index on the pk column allows O(log N) key lookups.
class Table {
    Schema      schema_;
    BufferPool* pool_;
    DiskManager* disk_;
    Logger*     logger_;

    // Page directory: page_ids used by this table (raw array)
    uint32_t* pageIds_;
    int       numPages_;
    int       pageIdCap_;

    uint32_t  totalRecords_;
    uint32_t  nextPageId_;   // monotonically increasing page id

    AVLTree   index_;        // primary key index: key -> {page_id, slot}

    char metaFile_[512];     // path to .meta file

    void growPageIds() {
        int nc = pageIdCap_ * 2;
        uint32_t* np = new uint32_t[nc];
        for (int i = 0; i < numPages_; i++) np[i] = pageIds_[i];
        delete[] pageIds_;
        pageIds_ = np;
        pageIdCap_ = nc;
    }

    Page* getWritablePage() {
        // Return last page if it has space
        if (numPages_ > 0) {
            uint32_t lastPg = pageIds_[numPages_ - 1];
            Page* p = pool_->getPage(schema_.tableName, lastPg);
            if (p->hasSpace()) return p;
        }
        // Allocate a new page
        uint32_t newPgId = nextPageId_++;
        Page* p = pool_->getPage(schema_.tableName, newPgId);
        p->clear();
        p->header()->page_id     = newPgId;
        p->header()->table_id    = 0;
        p->header()->num_records = 0;
        p->header()->record_size = (uint16_t)schema_.rowSize();
        safe_strcpy(p->header()->table_name, schema_.tableName, 32);
        p->setDirty(true);
        if (numPages_ >= pageIdCap_) growPageIds();
        pageIds_[numPages_++] = newPgId;
        return p;
    }

public:
    Table(const Schema& s, BufferPool* pool, DiskManager* disk, Logger* lg)
        : schema_(s), pool_(pool), disk_(disk), logger_(lg),
          numPages_(0), pageIdCap_(64), totalRecords_(0), nextPageId_(0)
    {
        pageIds_ = new uint32_t[pageIdCap_];
        snprintf(metaFile_, sizeof(metaFile_), "%s/%s.meta",
                 disk->getDataDir(), s.tableName);
        disk_->ensureFile(s.tableName);
        loadMetadata();
    }

    ~Table() {
        saveMetadata();
        pool_->flushAll();
        delete[] pageIds_;
    }

    const Schema& getSchema()       const { return schema_; }
    uint32_t      getTotalRecords() const { return totalRecords_; }
    const char*   getName()         const { return schema_.tableName; }

    // ── BULK INSERT (fast path, bypasses parser) ──────────────────────────────
    // Values must be provided as an array of strings matching schema column order.
    bool bulkInsert(const char values[][128], int numValues) {
        if (numValues != schema_.numCols) return false;
        Row row(schema_.numCols);
        for (int i = 0; i < schema_.numCols; i++)
            row.setColumn(i, makeValue(values[i], schema_.cols[i].type, schema_.cols[i].strLen));
        return insertRow(row);
    }

    // ── INSERT ────────────────────────────────────────────────────────────────
    bool insertRow(const Row& row) {
        Page* p = getWritablePage();
        if (!p) return false;

        char buf[1024];
        row.serialize(buf, schema_);
        int slot = p->insertRecord(buf, (uint16_t)schema_.rowSize());
        if (slot < 0) return false;

        pool_->markDirty(schema_.tableName, p->header()->page_id);

        // Update AVL index on pk column
        if (schema_.pkColIdx >= 0) {
            Value* pk = row.getColumn(schema_.pkColIdx);
            if (pk) {
                index_.insert(pk->toInt(),
                              (int)p->header()->page_id, slot);
            }
        }
        totalRecords_++;
        return true;
    }

    // ── O(log N) INDEX LOOKUP ─────────────────────────────────────────────────
    Row* findByKeyIndexed(int key) {
        AVLValue* loc = index_.search(key);
        if (!loc) return nullptr;
        Page* p = pool_->getPage(schema_.tableName, (uint32_t)loc->page_id);
        if (!p) return nullptr;
        const char* slot = p->getSlot(loc->slot);
        Row* r = new Row(schema_.numCols);
        r->deserialize(slot, schema_);
        return r;
    }

    // ── O(N) SEQUENTIAL SCAN ─────────────────────────────────────────────────
    Row* findByKeySequential(int key, int pkCol) {
        for (int pi = 0; pi < numPages_; pi++) {
            Page* p = pool_->getPage(schema_.tableName, pageIds_[pi]);
            if (!p) continue;
            int n = p->header()->num_records;
            for (int s = 0; s < n; s++) {
                const char* slot = p->getSlot(s);
                Row* r = new Row(schema_.numCols);
                r->deserialize(slot, schema_);
                Value* v = r->getColumn(pkCol);
                if (v && v->toInt() == key) return r;
                delete r;
            }
        }
        return nullptr;
    }

    // ── FULL TABLE SCAN returning array of matching rows ─────────────────────
    // Caller must free rows and the array.
    int scan(Row**& results, int maxRows = 100000) {
        results = new Row*[maxRows];
        int cnt = 0;
        for (int pi = 0; pi < numPages_ && cnt < maxRows; pi++) {
            Page* p = pool_->getPage(schema_.tableName, pageIds_[pi]);
            if (!p) continue;
            int n = p->header()->num_records;
            for (int s = 0; s < n && cnt < maxRows; s++) {
                Row* r = new Row(schema_.numCols);
                r->deserialize(p->getSlot(s), schema_);
                results[cnt++] = r;
            }
        }
        return cnt;
    }

    // ── FILTERED SCAN ────────────────────────────────────────────────────────
    // Returns rows for which filterFn returns true.
    // filterFn signature: bool(const Row&, const Schema&)
    template<typename Fn>
    int scanFilter(Row**& results, int maxRows, Fn filterFn) {
        results = new Row*[maxRows];
        int cnt = 0;
        for (int pi = 0; pi < numPages_ && cnt < maxRows; pi++) {
            Page* p = pool_->getPage(schema_.tableName, pageIds_[pi]);
            if (!p) continue;
            int n = p->header()->num_records;
            for (int s = 0; s < n && cnt < maxRows; s++) {
                Row* r = new Row(schema_.numCols);
                r->deserialize(p->getSlot(s), schema_);
                if (filterFn(*r, schema_))
                    results[cnt++] = r;
                else
                    delete r;
            }
        }
        return cnt;
    }

    // ── UPDATE a row by primary key ───────────────────────────────────────────
    bool updateRow(int key, int colIdx, Value* newVal) {
        AVLValue* loc = index_.search(key);
        if (!loc) return false;
        Page* p = pool_->getPage(schema_.tableName, (uint32_t)loc->page_id);
        if (!p) return false;
        Row r(schema_.numCols);
        r.deserialize(p->getSlot(loc->slot), schema_);
        r.setColumn(colIdx, newVal->clone());
        char buf[1024];
        r.serialize(buf, schema_);
        p->updateRecord(loc->slot, buf, (uint16_t)schema_.rowSize());
        pool_->markDirty(schema_.tableName, (uint32_t)loc->page_id);
        logger_->log("UPDATE on table %s: key=%d col=%s",
                     schema_.tableName, key, schema_.cols[colIdx].name);
        return true;
    }

    // ── REBUILD INDEX from disk ───────────────────────────────────────────────
    void buildIndex() {
        if (schema_.pkColIdx < 0) return;
        logger_->log("Building AVL index on %s.%s (%d records)...",
                     schema_.tableName,
                     schema_.cols[schema_.pkColIdx].name,
                     totalRecords_);
        for (int pi = 0; pi < numPages_; pi++) {
            Page* p = pool_->getPage(schema_.tableName, pageIds_[pi]);
            if (!p) continue;
            int n = p->header()->num_records;
            for (int s = 0; s < n; s++) {
                Row r(schema_.numCols);
                r.deserialize(p->getSlot(s), schema_);
                Value* pk = r.getColumn(schema_.pkColIdx);
                if (pk) index_.insert(pk->toInt(), (int)p->header()->page_id, s);
            }
        }
        logger_->log("Index built: %d nodes, height=%d",
                     index_.size(), index_.getHeight());
    }

    // ── METADATA PERSISTENCE ─────────────────────────────────────────────────
    void saveMetadata() {
        FILE* f = fopen(metaFile_, "w");
        if (!f) return;
        fprintf(f, "TABLE=%s\n",   schema_.tableName);
        fprintf(f, "RECORDS=%u\n", totalRecords_);
        fprintf(f, "NEXTPAGE=%u\n",nextPageId_);
        fprintf(f, "NUMPAGES=%u\n",numPages_);
        for (int i = 0; i < numPages_; i++)
            fprintf(f, "PAGE=%u\n", pageIds_[i]);
        fclose(f);
    }

    void loadMetadata() {
        FILE* f = fopen(metaFile_, "r");
        if (!f) return;
        char line[128];
        totalRecords_ = 0; nextPageId_ = 0; numPages_ = 0;
        int pagesRead = 0;
        while (fgets(line, sizeof(line), f)) {
            int len = (int)strlen(line);
            while (len > 0 && (line[len-1]=='\n'||line[len-1]=='\r')) line[--len]='\0';
            if (strncmp(line, "RECORDS=", 8) == 0) {
                totalRecords_ = (unsigned)atoi(line + 8);
            } else if (strncmp(line, "NEXTPAGE=", 9) == 0) {
                nextPageId_ = (unsigned)atoi(line + 9);
            } else if (strncmp(line, "NUMPAGES=", 9) == 0) {
                numPages_ = (unsigned)atoi(line + 9);
                if (numPages_ > pageIdCap_) {
                    delete[] pageIds_;
                    pageIdCap_ = numPages_ + 64;
                    pageIds_ = new uint32_t[pageIdCap_];
                }
            } else if (strncmp(line, "PAGE=", 5) == 0 && pagesRead < (int)numPages_) {
                pageIds_[pagesRead++] = (uint32_t)atoi(line + 5);
            }
        }
        fclose(f);
        if (totalRecords_ > 0) buildIndex();
    }

    void flush() {
        saveMetadata();
        pool_->flushAll();
    }

    // Getters for optimizer
    int getNumPages() const { return numPages_; }
    AVLTree& getIndex() { return index_; }
};
