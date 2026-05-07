#pragma once
#include "../common.h"
#include "../engine/NanoDBEngine.h"
#include "../Logger.h"
#include <cstdio>
#include <cstring>

// ─── BenchmarkRunner ──────────────────────────────────────────────────────────
// Runs all seven demo test cases and produces timing data for the report.
class BenchmarkRunner {
    NanoDBEngine* engine;
    Logger*       logger;

    // ── Print section banner ──────────────────────────────────────────────────
    void banner(const char* title) {
        logger->separator(title);
        printf("\n");
    }

    // ── Test Case A ───────────────────────────────────────────────────────────
    void testCaseA() {
        banner("TEST CASE A: Parser & Evaluator (Infix → Postfix → Filter)");
        const char* query =
            "SELECT * FROM customer WHERE "
            "(c_acctbal > 5000 AND c_mktsegment == \"BUILDING\") OR c_nationkey == 15";
        logger->log("Input query: %s", query);
        double t0 = getTimeMs();
        engine->executeQuery(query, true);
        double elapsed = getTimeMs() - t0;
        logger->log("Test Case A completed in %.4f ms", elapsed);
        printf("[TEST A DONE] %.4f ms\n\n", elapsed);
    }

    // ── Test Case B ───────────────────────────────────────────────────────────
    void testCaseB() {
        banner("TEST CASE B: Index Optimizer (Sequential vs AVL)");
        // Search for customer key 10000 (exists in generated data)
        engine->benchmarkScans("customer", 10000);
        logger->log("Test Case B completed.");
    }

    // ── Test Case C ───────────────────────────────────────────────────────────
    void testCaseC() {
        banner("TEST CASE C: 3-Table Join Optimizer (MST)");
        const char* query =
            "SELECT * FROM customer JOIN orders ON customer.c_custkey == orders.o_custkey "
            "JOIN lineitem ON orders.o_orderkey == lineitem.l_orderkey";
        logger->log("Input query: %s", query);
        double t0 = getTimeMs();
        engine->executeQuery(query, true);
        double elapsed = getTimeMs() - t0;
        logger->log("Test Case C completed in %.4f ms", elapsed);
        printf("[TEST C DONE] %.4f ms\n\n", elapsed);
    }

    // ── Test Case D ───────────────────────────────────────────────────────────
    void testCaseD() {
        banner("TEST CASE D: Memory Stress Test (LRU Eviction)");
        engine->memoryStressTest(50);
        logger->log("Test Case D completed.");
    }

    // ── Test Case E ───────────────────────────────────────────────────────────
    void testCaseE() {
        banner("TEST CASE E: Priority Queue Concurrency (Admin vs User)");
        engine->priorityQueueDemo();
        logger->log("Test Case E completed.");
    }

    // ── Test Case F ───────────────────────────────────────────────────────────
    void testCaseF() {
        banner("TEST CASE F: Deep Expression Tree Edge Case");
        const char* query =
            "SELECT * FROM orders WHERE "
            "((o_totalprice * 1.5) > 100000 AND (o_custkey % 2 == 0)) OR (o_orderstatus != \"O\")";
        logger->log("Input query: %s", query);
        double t0 = getTimeMs();
        engine->executeQuery(query, true);
        double elapsed = getTimeMs() - t0;
        logger->log("Test Case F completed in %.4f ms", elapsed);
        printf("[TEST F DONE] %.4f ms\n\n", elapsed);
    }

    // ── Test Case G ───────────────────────────────────────────────────────────
    void testCaseG() {
        banner("TEST CASE G: Durability & Persistence");
        engine->persistenceDemo();
        // Verify the inserted records are immediately queryable
        engine->verifyPersistence();
        logger->log("Test Case G completed.");
    }

    // ── Empirical Benchmarks (for Research Report) ────────────────────────────
    void empiricalBenchmarks() {
        banner("EMPIRICAL BENCHMARKS: Execution Time vs Data Size");

        // Scan benchmarks at different sizes
        int sizes[] = {1000, 10000, 100000};
        int numSizes = 3;

        for (int si = 0; si < numSizes; si++) {
            int N = sizes[si];
            // Simulate by scanning first N records of lineitem
            Table* t = engine->findTable("lineitem");
            if (!t) continue;

            double t0 = getTimeMs();
            Row** rows; int cnt = t->scan(rows, N);
            double scanMs = getTimeMs() - t0;
            for (int i = 0; i < cnt; i++) delete rows[i];
            delete[] rows;

            logger->log("Sequential scan (%dK records): %.4f ms", N/1000, scanMs);
            printf("  N=%6d  SeqScan=%.4f ms\n", N, scanMs);

            // Indexed lookup for same scale (random sample)
            double tIdx = getTimeMs();
            for (int k = 1; k <= (N < 1000 ? 10 : 50); k++) {
                int key = (k * (N / 50)) % N + 1;
                t->findByKeyIndexed(key);  // result discarded
            }
            double idxMs = (getTimeMs() - tIdx);
            logger->log("AVL index lookups (sample, N=%d): %.4f ms total", N, idxMs);
        }

        logger->log("Empirical benchmarks complete.");
    }

    // ── LRU page-fault profiling ──────────────────────────────────────────────
    void lruProfiling() {
        banner("MEMORY PROFILING: LRU Cache Page-Fault Rates");

        // Already tested in Test Case D; report here too
        logger->log("Buffer pool size: %d frames", DEFAULT_POOL_SIZE);
        logger->log("Stress pool size: %d frames (Test Case D config)", STRESS_POOL_SIZE);
        logger->log("Total evictions logged: %llu",
                    (unsigned long long)engine->getEvictionCount());
        printf("  LRU Evictions (cumulative): %llu\n\n",
               (unsigned long long)engine->getEvictionCount());
    }

public:
    BenchmarkRunner(NanoDBEngine* eng, Logger* lg)
        : engine(eng), logger(lg) {}

    void runAll() {
        logger->separator("STARTING ALL TEST CASES");
        testCaseA();
        testCaseB();
        testCaseC();
        testCaseD();
        testCaseE();
        testCaseF();
        testCaseG();
        empiricalBenchmarks();
        lruProfiling();
        logger->separator("ALL TEST CASES COMPLETE");
    }

    void runTestCase(char tc) {
        switch (tc) {
            case 'A': case 'a': testCaseA(); break;
            case 'B': case 'b': testCaseB(); break;
            case 'C': case 'c': testCaseC(); break;
            case 'D': case 'd': testCaseD(); break;
            case 'E': case 'e': testCaseE(); break;
            case 'F': case 'f': testCaseF(); break;
            case 'G': case 'g': testCaseG(); break;
            default: logger->warn("Unknown test case: %c", tc); break;
        }
    }
};
