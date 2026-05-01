#pragma once
#include "../common.h"
#include "Page.h"
#include "../Logger.h"
#include <cstring>
#ifdef _WIN32
#include <direct.h>
#define MKDIR(path) _mkdir(path)
#else
#include <sys/stat.h>
#define MKDIR(path) mkdir(path, 0755)
#endif

// ─── DiskManager ─────────────────────────────────────────────────────────────
// Handles raw binary serialization / deserialization of Pages to/from disk.
// Each table has its own .dat file. Pages are stored sequentially by page_id.
class DiskManager {
    char   dataDir[256];
    Logger* logger;

public:
    DiskManager(const char* dir, Logger* lg)
        : logger(lg)
    {
        safe_strcpy(dataDir, dir, 256);
        // Ensure directory exists
        MKDIR(dataDir);
    }

    void buildPath(char* out, int outLen, const char* tableName) const {
        snprintf(out, outLen, "%s/%s.dat", dataDir, tableName);
    }

    bool fileExists(const char* tableName) const {
        char path[512];
        buildPath(path, sizeof(path), tableName);
        FILE* f = fopen(path, "rb");
        if (f) { fclose(f); return true; }
        return false;
    }

    // Create empty file if it doesn't exist
    void ensureFile(const char* tableName) {
        if (!fileExists(tableName)) {
            char path[512];
            buildPath(path, sizeof(path), tableName);
            FILE* f = fopen(path, "wb");
            if (f) fclose(f);
        }
    }

    // Write a page to disk (binary, at exact offset = page_id * PAGE_SIZE)
    void writePage(const char* tableName, uint32_t page_id, Page* page) {
        char path[512];
        buildPath(path, sizeof(path), tableName);
        FILE* f = fopen(path, "r+b");
        if (!f) f = fopen(path, "w+b");
        if (!f) {
            if (logger) logger->warn("DiskManager: cannot open %s for write", path);
            return;
        }
        page->writeToDisk(f, page_id);
        fclose(f);
        page->setDirty(false);
    }

    // Read a page from disk; returns false if page not found (new page)
    bool readPage(const char* tableName, uint32_t page_id, Page* page) {
        char path[512];
        buildPath(path, sizeof(path), tableName);
        FILE* f = fopen(path, "rb");
        if (!f) { page->clear(); return false; }
        bool ok = page->readFromDisk(f, page_id);
        fclose(f);
        return ok;
    }

    // Get the number of pages already stored for a table
    uint32_t getPageCount(const char* tableName) const {
        char path[512];
        buildPath(path, sizeof(path), tableName);
        FILE* f = fopen(path, "rb");
        if (!f) return 0;
        fseek(f, 0, SEEK_END);
        long sz = ftell(f);
        fclose(f);
        return (uint32_t)(sz / PAGE_SIZE);
    }

    const char* getDataDir() const { return dataDir; }
};
