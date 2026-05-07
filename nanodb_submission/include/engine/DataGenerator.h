#pragma once
#include "../common.h"
#include "../engine/NanoDBEngine.h"
#include "../schema/Table.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>

// ─── TPC-H Synthetic Data Generator ──────────────────────────────────────────
// Uses fast direct Table::bulkInsert() to bypass the query parser for speed.
// 20,000 customers, 30,000 orders, 50,000 lineitems = 100,000 records total.
class DataGenerator {
    static const int NUM_CUSTOMERS = 20000;
    static const int NUM_ORDERS    = 30000;
    static const int NUM_LINEITEMS = 50000;

    Logger* logger;

    // Deterministic LCG random (avoids srand dependency)
    struct LCG {
        uint32_t state;
        explicit LCG(uint32_t seed = 12345) : state(seed) {}
        uint32_t next() { state = state * 1664525u + 1013904223u; return state; }
        int    nextInt(int lo, int hi)  { return lo + (int)(next() % (uint32_t)(hi - lo + 1)); }
        float  nextFloat(float lo, float hi) {
            float r = (float)(next() & 0xFFFF) / 65535.0f;
            return lo + r * (hi - lo);
        }
    };

    static const char* mktsegments[];
    static const char* orderstatus[];
    static const char* orderprior[];
    static const char* returnflag[];
    static const char* linestatus[];

    void generateCustomers(NanoDBEngine* engine, LCG& rng) {
        logger->log("Generating %d customers...", NUM_CUSTOMERS);
        Table* t = engine->findTable("customer");
        if (!t) { logger->warn("customer table not found"); return; }

        char vals[20][128];
        for (int i = 1; i <= NUM_CUSTOMERS; i++) {
            snprintf(vals[0],  sizeof(vals[0]),  "%d", i);
            snprintf(vals[1],  sizeof(vals[1]),  "Customer#%06d", i);
            snprintf(vals[2],  sizeof(vals[2]),  "Addr%dSt%d", rng.nextInt(1,999), rng.nextInt(1,99));
            snprintf(vals[3],  sizeof(vals[3]),  "%d", rng.nextInt(0, 24));
            snprintf(vals[4],  sizeof(vals[4]),  "%02d-%03d-%04d",
                     rng.nextInt(10,99), rng.nextInt(100,999), rng.nextInt(1000,9999));
            snprintf(vals[5],  sizeof(vals[5]),  "%.2f", rng.nextFloat(-999.99f, 9999.99f));
            snprintf(vals[6],  sizeof(vals[6]),  "%s",  mktsegments[rng.next() % 5]);
            snprintf(vals[7],  sizeof(vals[7]),  "comment%d", rng.nextInt(1,9999));
            t->bulkInsert((const char(*)[128])vals, 8);

            if (i % 5000 == 0)
                logger->log("  Customers: %d / %d", i, NUM_CUSTOMERS);
        }
        logger->log("Customers generated: %d", NUM_CUSTOMERS);
    }

    void generateOrders(NanoDBEngine* engine, LCG& rng) {
        logger->log("Generating %d orders...", NUM_ORDERS);
        Table* t = engine->findTable("orders");
        if (!t) { logger->warn("orders table not found"); return; }

        char vals[20][128];
        for (int i = 1; i <= NUM_ORDERS; i++) {
            int y = rng.nextInt(1992, 1998);
            int m = rng.nextInt(1, 12);
            int d = rng.nextInt(1, 28);
            snprintf(vals[0], sizeof(vals[0]),  "%d", i);
            snprintf(vals[1], sizeof(vals[1]),  "%d", rng.nextInt(1, NUM_CUSTOMERS));
            snprintf(vals[2], sizeof(vals[2]),  "%s", orderstatus[rng.next() % 3]);
            snprintf(vals[3], sizeof(vals[3]),  "%.2f", rng.nextFloat(100.0f, 500000.0f));
            snprintf(vals[4], sizeof(vals[4]),  "%04d-%02d-%02d", y, m, d);
            snprintf(vals[5], sizeof(vals[5]),  "%s", orderprior[rng.next() % 5]);
            snprintf(vals[6], sizeof(vals[6]),  "Clerk#%06d", rng.nextInt(1, 1000));
            snprintf(vals[7], sizeof(vals[7]),  "%d", rng.nextInt(0, 2));
            snprintf(vals[8], sizeof(vals[8]),  "order_cmnt_%d", rng.nextInt(1, 9999));
            t->bulkInsert((const char(*)[128])vals, 9);

            if (i % 10000 == 0)
                logger->log("  Orders: %d / %d", i, NUM_ORDERS);
        }
        logger->log("Orders generated: %d", NUM_ORDERS);
    }

    void generateLineitems(NanoDBEngine* engine, LCG& rng) {
        logger->log("Generating %d lineitems...", NUM_LINEITEMS);
        Table* t = engine->findTable("lineitem");
        if (!t) { logger->warn("lineitem table not found"); return; }

        char vals[20][128];
        for (int i = 1; i <= NUM_LINEITEMS; i++) {
            int y = rng.nextInt(1992, 1998);
            int m = rng.nextInt(1, 12);
            int d = rng.nextInt(1, 28);
            snprintf(vals[0],  sizeof(vals[0]),  "%d", rng.nextInt(1, NUM_ORDERS));
            snprintf(vals[1],  sizeof(vals[1]),  "%d", rng.nextInt(1, 2000));
            snprintf(vals[2],  sizeof(vals[2]),  "%d", rng.nextInt(1, 1000));
            snprintf(vals[3],  sizeof(vals[3]),  "%d", rng.nextInt(1, 7));
            snprintf(vals[4],  sizeof(vals[4]),  "%.2f", rng.nextFloat(1.0f, 50.0f));
            snprintf(vals[5],  sizeof(vals[5]),  "%.2f", rng.nextFloat(100.0f, 100000.0f));
            snprintf(vals[6],  sizeof(vals[6]),  "%.4f", rng.nextFloat(0.0f, 0.10f));
            snprintf(vals[7],  sizeof(vals[7]),  "%.4f", rng.nextFloat(0.0f, 0.08f));
            snprintf(vals[8],  sizeof(vals[8]),  "%s",   returnflag[rng.next() % 3]);
            snprintf(vals[9],  sizeof(vals[9]),  "%s",   linestatus[rng.next() % 2]);
            snprintf(vals[10], sizeof(vals[10]), "%04d-%02d-%02d", y, m, d);
            snprintf(vals[11], sizeof(vals[11]), "li_cmnt_%d", rng.nextInt(1, 9999));
            t->bulkInsert((const char(*)[128])vals, 12);

            if (i % 10000 == 0)
                logger->log("  Lineitems: %d / %d", i, NUM_LINEITEMS);
        }
        logger->log("Lineitems generated: %d", NUM_LINEITEMS);
    }

public:
    explicit DataGenerator(Logger* lg) : logger(lg) {}

    bool isDataPresent(NanoDBEngine* engine) {
        Table* t = engine->findTable("customer");
        return t && t->getTotalRecords() >= 100;
    }

    void generate(NanoDBEngine* engine) {
        if (isDataPresent(engine)) {
            logger->log("Data already present (%u customers) — skipping generation.",
                        engine->findTable("customer")->getTotalRecords());
            return;
        }
        logger->separator("TPC-H Data Generation (bulk fast-path)");
        LCG rng(42);
        double t0 = getTimeMs();
        generateCustomers(engine, rng);
        generateOrders(engine, rng);
        generateLineitems(engine, rng);
        engine->flush();
        double elapsed = getTimeMs() - t0;
        logger->log("Data generation complete: 100,000 records in %.2f ms (%.1f rec/ms)",
                    elapsed, 100000.0 / (elapsed > 0 ? elapsed : 1));
        logger->separator(nullptr);
    }

    static int getNumCustomers() { return NUM_CUSTOMERS; }
    static int getNumOrders()    { return NUM_ORDERS; }
    static int getNumLineitems() { return NUM_LINEITEMS; }
};

const char* DataGenerator::mktsegments[] = {"AUTOMOBILE","BUILDING","FURNITURE","HOUSEHOLD","MACHINERY"};
const char* DataGenerator::orderstatus[] = {"F","O","P"};
const char* DataGenerator::orderprior[]  = {"1-URGENT","2-HIGH","3-MEDIUM","4-NOT SPECIFIED","5-LOW"};
const char* DataGenerator::returnflag[]  = {"A","N","R"};
const char* DataGenerator::linestatus[]  = {"F","O"};
