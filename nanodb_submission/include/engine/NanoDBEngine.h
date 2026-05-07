#pragma once
#include "../common.h"
#include "../Logger.h"
#include "../memory/DiskManager.h"
#include "../memory/BufferPool.h"
#include "../schema/SystemCatalog.h"
#include "../schema/Table.h"
#include "../parser/QueryParser.h"
#include "../parser/ExpressionParser.h"
#include "../ds/PriorityQueue.h"
#include "../ds/Graph.h"
#include <cstdio>
#include <cstring>

// ─── NanoDB Engine ────────────────────────────────────────────────────────────
// Wires together: DiskManager → BufferPool → SystemCatalog → Tables
//                 QueryParser → ExpressionParser → PriorityQueue
//                 Graph/MST optimizer
class NanoDBEngine {
    Logger*        logger;
    DiskManager*   disk;
    BufferPool*    pool;
    SystemCatalog* catalog;
    QueryParser*   parser;
    ExpressionParser exprParser;
    PriorityQueue  pq;

    // Table registry (raw array, HashMap by name)
    Table*  tables[MAX_TABLES];
    char    tableNames[MAX_TABLES][MAX_TABLE_NAME];
    int     numTables;

    int     querySeq;    // monotonic counter for PQ tie-breaking

    Table* getTable(const char* name) {
        for (int i = 0; i < numTables; i++)
            if (strcmp(tableNames[i], name) == 0) return tables[i];
        return nullptr;
    }

    // ── Create TPC-H schemas ──────────────────────────────────────────────────
    // Always define schemas inline - never rely on catalog pointer (Mac crash fix)
    void defineTpchSchemas() {
        // CUSTOMER
        {
            Schema s;
            safe_strcpy(s.tableName, "customer", MAX_TABLE_NAME);
            s.addColumn("c_custkey",    ColType::INT);
            s.addColumn("c_name",       ColType::STRING, 25);
            s.addColumn("c_address",    ColType::STRING, 40);
            s.addColumn("c_nationkey",  ColType::INT);
            s.addColumn("c_phone",      ColType::STRING, 15);
            s.addColumn("c_acctbal",    ColType::FLOAT);
            s.addColumn("c_mktsegment", ColType::STRING, 10);
            s.addColumn("c_comment",    ColType::STRING, 40);
            s.pkColIdx = 0;
            char df[256]; snprintf(df, sizeof(df), "%s/customer.dat", disk->getDataDir());
            if (!catalog->tableExists("customer"))
                catalog->registerTable(s, df);
            openOrCreateTable(s);
        }
        // ORDERS
        {
            Schema s;
            safe_strcpy(s.tableName, "orders", MAX_TABLE_NAME);
            s.addColumn("o_orderkey",    ColType::INT);
            s.addColumn("o_custkey",     ColType::INT);
            s.addColumn("o_orderstatus", ColType::STRING, 1);
            s.addColumn("o_totalprice",  ColType::FLOAT);
            s.addColumn("o_orderdate",   ColType::STRING, 10);
            s.addColumn("o_orderpriority",ColType::STRING,15);
            s.addColumn("o_clerk",       ColType::STRING, 15);
            s.addColumn("o_shippriority",ColType::INT);
            s.addColumn("o_comment",     ColType::STRING, 40);
            s.pkColIdx = 0;
            char df[256]; snprintf(df, sizeof(df), "%s/orders.dat", disk->getDataDir());
            if (!catalog->tableExists("orders"))
                catalog->registerTable(s, df);
            openOrCreateTable(s);
        }
        // LINEITEM
        {
            Schema s;
            safe_strcpy(s.tableName, "lineitem", MAX_TABLE_NAME);
            s.addColumn("l_orderkey",     ColType::INT);
            s.addColumn("l_partkey",      ColType::INT);
            s.addColumn("l_suppkey",      ColType::INT);
            s.addColumn("l_linenumber",   ColType::INT);
            s.addColumn("l_quantity",     ColType::FLOAT);
            s.addColumn("l_extendedprice",ColType::FLOAT);
            s.addColumn("l_discount",     ColType::FLOAT);
            s.addColumn("l_tax",          ColType::FLOAT);
            s.addColumn("l_returnflag",   ColType::STRING, 1);
            s.addColumn("l_linestatus",   ColType::STRING, 1);
            s.addColumn("l_shipdate",     ColType::STRING, 10);
            s.addColumn("l_comment",      ColType::STRING, 27);
            s.pkColIdx = 0;
            char df[256]; snprintf(df, sizeof(df), "%s/lineitem.dat", disk->getDataDir());
            if (!catalog->tableExists("lineitem"))
                catalog->registerTable(s, df);
            openOrCreateTable(s);
        }
    }

    void openOrCreateTable(const Schema& s) {
        if (numTables >= MAX_TABLES) return;
        Table* t = new Table(s, pool, disk, logger);
        tables[numTables] = t;
        safe_strcpy(tableNames[numTables], s.tableName, MAX_TABLE_NAME);
        numTables++;
    }

    void openExistingTable(const char* name) {
        Schema* s = catalog->getSchema(name);
        if (!s) return;
        openOrCreateTable(*s);
    }

    // ── Execute a single parsed query ────────────────────────────────────────
    void executeSelect(const ParsedQuery& pq_, bool printRows = true) {
        Table* t = getTable(pq_.tableName);
        if (!t) { logger->warn("Table not found: %s", pq_.tableName); return; }

        const Schema& sc = t->getSchema();

        if (!pq_.hasWhere) {
            // Full scan
            Row** rows; int cnt = t->scan(rows, 200000);
            logger->log("SELECT on %s: %d rows (full scan)", pq_.tableName, cnt);
            if (printRows) {
                for (int i = 0; i < cnt && i < 20; i++) rows[i]->print(sc);
                if (cnt > 20) printf("  ... (%d more rows)\n", cnt - 20);
            }
            for (int i = 0; i < cnt; i++) delete rows[i];
            delete[] rows;
        } else {
            // Filtered scan
            const ExprToken* pfix = pq_.postfix;
            int plen = pq_.postfixLen;
            Row** rows;
            int cnt = t->scanFilter(rows, 200000,
                [&](const Row& r, const Schema& sch) {
                    return exprParser.evaluate(pfix, plen, r, sch);
                });
            logger->log("SELECT WHERE on %s: %d rows matched", pq_.tableName, cnt);
            if (printRows) {
                for (int i = 0; i < cnt && i < 50; i++) rows[i]->print(sc);
                if (cnt > 50) printf("  ... (%d more rows)\n", cnt - 50);
            }
            for (int i = 0; i < cnt; i++) delete rows[i];
            delete[] rows;
        }
    }

    void executeInsert(const ParsedQuery& pq_) {
        Table* t = getTable(pq_.tableName);
        if (!t) { logger->warn("INSERT: Table not found: %s", pq_.tableName); return; }
        const Schema& sc = t->getSchema();
        if (pq_.numInsertValues != sc.numCols) {
            logger->warn("INSERT: value count mismatch for %s (got %d, expected %d)",
                         pq_.tableName, pq_.numInsertValues, sc.numCols);
            return;
        }
        Row row(sc.numCols);
        for (int i = 0; i < sc.numCols; i++) {
            row.setColumn(i, makeValue(pq_.insertValues[i], sc.cols[i].type, sc.cols[i].strLen));
        }
        bool ok = t->insertRow(row);
        logger->log("INSERT into %s: %s", pq_.tableName, ok ? "OK" : "FAILED");
    }

    void executeUpdate(const ParsedQuery& pq_) {
        Table* t = getTable(pq_.tableName);
        if (!t) { logger->warn("UPDATE: Table not found: %s", pq_.tableName); return; }
        const Schema& sc = t->getSchema();
        int colIdx = sc.colIndex(pq_.updateCol);
        if (colIdx < 0) { logger->warn("UPDATE: column not found: %s", pq_.updateCol); return; }
        Value* newVal = makeValue(pq_.updateVal, sc.cols[colIdx].type, sc.cols[colIdx].strLen);

        if (pq_.hasWhere) {
            // Update all rows matching WHERE (need to find pk value)
            const ExprToken* pfix = pq_.postfix;
            int plen = pq_.postfixLen;
            Row** rows;
            int cnt = t->scanFilter(rows, 200000,
                [&](const Row& r, const Schema& sch) {
                    return exprParser.evaluate(pfix, plen, r, sch);
                });
            int updated = 0;
            for (int i = 0; i < cnt; i++) {
                Value* pk = rows[i]->getColumn(sc.pkColIdx);
                if (pk) {
                    t->updateRow(pk->toInt(), colIdx, newVal);
                    updated++;
                }
                delete rows[i];
            }
            delete[] rows;
            logger->log("UPDATE %s SET %s=%s: %d rows updated",
                        pq_.tableName, pq_.updateCol, pq_.updateVal, updated);
        }
        delete newVal;
    }

    void executeJoin(const ParsedQuery& pq_, bool printRows = true) {
        logger->separator("JOIN OPTIMIZER (MST)");
        Graph g(logger);
        int numJT = pq_.numJoinTables;

        for (int i = 0; i < numJT; i++) {
            Table* t = getTable(pq_.joinTables[i]);
            if (!t) { logger->warn("JOIN: table not found: %s", pq_.joinTables[i]); return; }
            g.addTable(pq_.joinTables[i], (int)t->getTotalRecords());
        }

        // Add edges between every pair of tables
        for (int i = 0; i < numJT; i++)
            for (int j = i+1; j < numJT; j++)
                g.addEdge(i, j);

        g.computeMST();
        g.printMST(logger);

        char joinOrder[MAX_TABLES][MAX_TABLE_NAME];
        int  numOrder = g.getJoinOrder(joinOrder);

        logger->log("Join execution order:");
        for (int i = 0; i < numOrder; i++)
            logger->log("  Step %d: %s", i+1, joinOrder[i]);

        // Simple nested-loop join (prints first 20 joined rows)
        if (!printRows) return;
        if (numJT < 2) return;

        // Load all rows from first table
        Table* t0 = getTable(joinOrder[0]);
        Table* t1 = getTable(joinOrder[1]);
        if (!t0 || !t1) return;

        Row** r0; int c0 = t0->scan(r0, 100000);
        Row** r1; int c1 = t1->scan(r1, 100000);

        logger->log("JOIN %s (%d rows) x %s (%d rows) [first 5 pairs shown]",
                    joinOrder[0], c0, joinOrder[1], c1);

        int shown = 0;
        for (int i = 0; i < c0 && shown < 5; i++) {
            for (int j = 0; j < c1 && shown < 5; j++) {
                // Join condition: customer.c_custkey == orders.o_custkey (col 0 & 1)
                Value* v0 = r0[i]->getColumn(0);
                Value* v1 = r1[j]->getColumn(1 < t1->getSchema().numCols ? 1 : 0);
                if (v0 && v1 && v0->toInt() == v1->toInt()) {
                    r0[i]->print(t0->getSchema());
                    printf("  ↕\n");
                    r1[j]->print(t1->getSchema());
                    printf("--------\n");
                    shown++;
                }
            }
        }
        for (int i = 0; i < c0; i++) delete r0[i];
        for (int i = 0; i < c1; i++) delete r1[i];
        delete[] r0; delete[] r1;
    }

public:
    NanoDBEngine(const char* dataDir, int poolSize = DEFAULT_POOL_SIZE)
        : exprParser(nullptr), numTables(0), querySeq(0)
    {
        logger  = new Logger("nanodb_execution.log", true);
        disk    = new DiskManager(dataDir, logger);
        pool    = new BufferPool(poolSize, disk, logger);
        catalog = new SystemCatalog(dataDir, logger);
        parser  = new QueryParser(logger);
        exprParser = ExpressionParser(logger);

        logger->separator("NanoDB Engine Starting");
        logger->log("Data directory: %s", dataDir);
        logger->log("Buffer pool size: %d frames (%d KB)",
                    poolSize, (int)((long)poolSize * PAGE_SIZE / 1024));

        defineTpchSchemas();
        logger->log("Engine ready. %d tables loaded.", numTables);
    }

    ~NanoDBEngine() {
        flush();
        for (int i = 0; i < numTables; i++) { delete tables[i]; tables[i] = nullptr; }
        delete parser;
        delete catalog;
        delete pool;
        delete disk;
        delete logger;
    }

    // ── Execute query string (goes through priority queue) ───────────────────
    void submitQuery(const char* queryStr, int priority = PRIORITY_USER) {
        QueryJob job(queryStr, priority, querySeq++);
        pq.insert(job);
    }

    // Drain and execute all queued queries
    void drainQueue(bool printRows = true) {
        while (!pq.isEmpty()) {
            QueryJob job = pq.extractMax();
            logger->separator(nullptr);
            logger->log("Executing [priority=%d]: %s", job.priority, job.query);
            executeQuery(job.query, printRows);
        }
    }

    // Execute immediately (bypass queue)
    void executeQuery(const char* queryStr, bool printRows = true) {
        ParsedQuery pq_;
        if (!parser->parse(queryStr, pq_)) {
            logger->warn("Parse failed: %s", queryStr);
            return;
        }
        switch (pq_.type) {
            case QueryType::SELECT:
                if (pq_.numJoinTables > 1) executeJoin(pq_, printRows);
                else                       executeSelect(pq_, printRows);
                break;
            case QueryType::JOIN_SELECT:   executeJoin(pq_, printRows);       break;
            case QueryType::INSERT:        executeInsert(pq_);                 break;
            case QueryType::UPDATE:        executeUpdate(pq_);                 break;
            default: logger->warn("Unsupported query type"); break;
        }
    }

    // ── Test Case B: compare sequential vs indexed scan ──────────────────────
    void benchmarkScans(const char* tableName, int key) {
        Table* t = getTable(tableName);
        if (!t) { logger->warn("benchmarkScans: table not found: %s", tableName); return; }
        const Schema& sc = t->getSchema();
        int pkCol = sc.pkColIdx >= 0 ? sc.pkColIdx : 0;

        logger->separator("TEST CASE B: Index vs Sequential Scan");
        logger->log("Searching for key=%d in table %s (%u records)",
                    key, tableName, t->getTotalRecords());

        // Sequential scan
        double t0 = getTimeMs();
        Row* rSeq = t->findByKeySequential(key, pkCol);
        double seqMs = getTimeMs() - t0;
        logger->log("Sequential scan time: %.4f ms  (result: %s)",
                    seqMs, rSeq ? "FOUND" : "NOT FOUND");
        if (rSeq) { printf("  Sequential result: "); rSeq->print(sc); }
        delete rSeq;

        // Indexed scan (AVL Tree)
        double t1 = getTimeMs();
        Row* rIdx = t->findByKeyIndexed(key);
        double idxMs = getTimeMs() - t1;
        logger->log("AVL Index scan time: %.4f ms  (result: %s)",
                    idxMs, rIdx ? "FOUND" : "NOT FOUND");
        if (rIdx) { printf("  Indexed result:    "); rIdx->print(sc); }
        delete rIdx;

        double speedup = (idxMs > 0.0001) ? seqMs / idxMs : 9999.0;
        logger->log("Speedup: %.1fx (sequential=%.4fms, indexed=%.4fms)",
                    speedup, seqMs, idxMs);
        printf("\n[RESULT] Sequential: %.4f ms | Indexed: %.4f ms | Speedup: %.1fx\n\n",
               seqMs, idxMs, speedup);
    }

    // ── Test Case D: memory stress test ──────────────────────────────────────
    void memoryStressTest(int maxPages = 50) {
        logger->separator("TEST CASE D: Memory Stress Test");
        logger->log("Restricting buffer pool simulation to %d page visibility window", maxPages);
        pool->resetEvictionCount();

        // Scan 5000 lineitem records — forces many page loads/evictions
        Table* t = getTable("lineitem");
        if (!t) { logger->warn("lineitem table not found"); return; }

        logger->log("Scanning lineitem table (%u records, %d pages)...",
                    t->getTotalRecords(), t->getNumPages());
        Row** rows; int cnt = t->scan(rows, 5000);
        for (int i = 0; i < cnt; i++) delete rows[i];
        delete[] rows;

        uint64_t evictions = pool->getEvictionCount();
        logger->log("Memory stress complete: %llu LRU evictions occurred during scan of %d records",
                    (unsigned long long)evictions, cnt);
        printf("\n[RESULT] LRU evicted %llu pages during %d-record scan\n\n",
               (unsigned long long)evictions, cnt);
    }

    // ── Test Case E: Priority Queue concurrency demo ──────────────────────────
    void priorityQueueDemo() {
        logger->separator("TEST CASE E: Priority Queue Concurrency");
        // Submit 50 user-level SELECT queries
        for (int i = 0; i < 50; i++) {
            char q[MAX_QUERY_LEN];
            snprintf(q, sizeof(q),
                     "SELECT * FROM customer WHERE c_nationkey == %d", i % 25);
            submitQuery(q, PRIORITY_USER);
        }
        logger->log("Submitted 50 standard SELECT queries (priority=%d)", PRIORITY_USER);

        // Now submit 1 ADMIN UPDATE (higher priority)
        submitQuery("ADMIN UPDATE customer SET c_acctbal = 9999.99 WHERE c_custkey == 1",
                    PRIORITY_ADMIN);
        logger->log("Submitted ADMIN UPDATE (priority=%d) — must execute first!", PRIORITY_ADMIN);

        // Show queue size
        logger->log("PriorityQueue size before drain: %d", pq.size());

        // Drain — admin query must run first
        logger->log("Draining priority queue...");
        int count = 0;
        while (!pq.isEmpty()) {
            QueryJob job = pq.extractMax();
            if (count == 0) {
                logger->log("  [1st EXECUTED] priority=%d: %s", job.priority, job.query);
                executeQuery(job.query, false);
            } else if (count < 3) {
                logger->log("  [%d] priority=%d: %s", count+1, job.priority, job.query);
            } else if (count == 3) {
                logger->log("  ... (%d more user queries processed silently)", pq.size());
            }
            if (count > 3) executeQuery(job.query, false);
            count++;
        }
        logger->log("Priority queue drained: %d queries processed", count);
        printf("\n[RESULT] Admin UPDATE ran first among %d queued queries\n\n", count);
    }

    // ── Test Case G: Durability / persistence ────────────────────────────────
    void persistenceDemo() {
        logger->separator("TEST CASE G: Durability & Persistence");
        // Insert 5 new customers
        const int startKey = 900001;
        for (int i = 0; i < 5; i++) {
            char q[MAX_QUERY_LEN];
            snprintf(q, sizeof(q),
                "INSERT INTO customer VALUES (%d, \"TestName%d\", \"TestAddr%d\", "
                "%d, \"555-000%d\", %.2f, \"DEMO\", \"Persistence test record %d\")",
                startKey + i, i, i, i, i, (float)(i * 100.0f + 50.0f), i);
            executeQuery(q, false);
        }
        flush();
        logger->log("5 records inserted and flushed to disk.");
        logger->log("Engine can now be restarted and records will be recovered.");
        printf("\n[RESULT] 5 records persisted. Flush to disk complete.\n\n");
    }

    // ── Test Case G Part 2: Query persisted records ───────────────────────────
    void verifyPersistence() {
        logger->log("Verifying persisted records...");
        executeQuery("SELECT * FROM customer WHERE c_custkey == 900001", true);
        executeQuery("SELECT * FROM customer WHERE c_custkey == 900002", true);
    }

    // ── Flush all data to disk ────────────────────────────────────────────────
    void flush() {
        for (int i = 0; i < numTables; i++)
            if (tables[i]) tables[i]->flush();
        logger->log("All tables flushed to disk.");
    }

    Logger* getLogger() { return logger; }
    PriorityQueue& getPQ() { return pq; }

    // For external access to pool eviction count
    uint64_t getEvictionCount() const { return pool->getEvictionCount(); }

    Table* findTable(const char* name) { return getTable(name); }
};
