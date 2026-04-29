#pragma once
#include "../common.h"
#include "Row.h"
#include "../ds/HashMap.h"
#include "../Logger.h"

// ─── Catalog Entry ────────────────────────────────────────────────────────────
struct CatalogEntry {
    Schema schema;
    char   dataFile[256];
    CatalogEntry() { dataFile[0] = '\0'; }
};

// ─── System Catalog ───────────────────────────────────────────────────────────
// Maps table_name -> CatalogEntry using a custom HashMap.
// Provides O(1) amortized lookup for table metadata.
class SystemCatalog {
    HashMap<CatalogEntry> map_;
    Logger* logger_;
    char    catalogFile_[512];

public:
    SystemCatalog(const char* dataDir, Logger* lg)
        : map_(HASH_BUCKETS), logger_(lg)
    {
        snprintf(catalogFile_, sizeof(catalogFile_), "%s/catalog.txt", dataDir);
        load();
    }

    void registerTable(const Schema& schema, const char* dataFile) {
        CatalogEntry e;
        e.schema = schema;
        safe_strcpy(e.dataFile, dataFile, 256);
        map_.put(schema.tableName, e);
        save();
        if (logger_)
            logger_->log("SystemCatalog: registered table '%s' -> '%s'",
                         schema.tableName, dataFile);
    }

    CatalogEntry* getEntry(const char* name) {
        return map_.get(name);
    }

    Schema* getSchema(const char* name) {
        CatalogEntry* e = map_.get(name);
        return e ? &e->schema : nullptr;
    }

    bool tableExists(const char* name) const {
        return map_.contains(name);
    }

    void listTables() const {
        printf("=== System Catalog ===\n");
        map_.forEach([](const char* key, const CatalogEntry& val) {
            printf("  Table: %-20s  File: %s\n", key, val.dataFile);
        });
        printf("=====================\n");
    }

    // ── Persistence: simple text format ──────────────────────────────────────
    void save() {
        FILE* f = fopen(catalogFile_, "w");
        if (!f) return;
        map_.forEach([&](const char* key, const CatalogEntry& val) {
            const Schema& s = val.schema;
            fprintf(f, "TABLE %s %d %d\n", s.tableName, s.numCols, s.pkColIdx);
            for (int i = 0; i < s.numCols; i++) {
                int t = (s.cols[i].type == ColType::INT) ? 0 :
                        (s.cols[i].type == ColType::FLOAT) ? 1 : 2;
                fprintf(f, "  COL %s %d %d\n", s.cols[i].name, t, s.cols[i].strLen);
            }
            fprintf(f, "  FILE %s\n", val.dataFile);
        });
        fclose(f);
    }

    void load() {
        FILE* f = fopen(catalogFile_, "r");
        if (!f) return;
        char line[512];
        CatalogEntry cur;
        bool inTable = false;
        while (fgets(line, sizeof(line), f)) {
            // Remove trailing newline
            int len = strlen(line);
            while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r'))
                line[--len] = '\0';

            if (strncmp(line, "TABLE ", 6) == 0) {
                cur = CatalogEntry();
                char tname[MAX_TABLE_NAME];
                int nc, pk;
                sscanf(line + 6, "%31s %d %d", tname, &nc, &pk);
                safe_strcpy(cur.schema.tableName, tname, MAX_TABLE_NAME);
                cur.schema.numCols  = nc;
                cur.schema.pkColIdx = pk;
                inTable = true;
            } else if (inTable && strncmp(line, "  COL ", 6) == 0) {
                char cname[MAX_COL_NAME];
                int t, sl;
                sscanf(line + 6, "%31s %d %d", cname, &t, &sl);
                // find next empty slot
                for (int i = 0; i < MAX_COLUMNS; i++) {
                    if (cur.schema.cols[i].name[0] == '\0') {
                        safe_strcpy(cur.schema.cols[i].name, cname, MAX_COL_NAME);
                        cur.schema.cols[i].type   = (t == 0) ? ColType::INT :
                                                    (t == 1) ? ColType::FLOAT : ColType::STRING;
                        cur.schema.cols[i].strLen = sl;
                        break;
                    }
                }
            } else if (inTable && strncmp(line, "  FILE ", 7) == 0) {
                safe_strcpy(cur.dataFile, line + 7, 256);
                map_.put(cur.schema.tableName, cur);
                inTable = false;
            }
        }
        fclose(f);
    }
};
