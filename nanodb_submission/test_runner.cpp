#include "include/common.h"
#include "include/engine/NanoDBEngine.h"
#include "include/engine/DataGenerator.h"
#include "include/engine/BenchmarkRunner.h"
#include <cstdio>
#include <cstring>

// ─── Test Runner ─────────────────────────────────────────────────────────────
// Reads queries.txt and executes each line through the NanoDB engine.
// Produces nanodb_execution.log with full trace of all operations.
int main(int argc, char* argv[]) {
    printf("╔══════════════════════════════════════════════════════════╗\n");
    printf("║       NanoDB — Automated Test Runner                    ║\n");
    printf("║   Reads queries.txt and executes all 50 queries          ║\n");
    printf("╚══════════════════════════════════════════════════════════╝\n\n");

    const char* dataDir      = "data";
    const char* workloadFile = "queries.txt";

    if (argc >= 2) dataDir      = argv[1];
    if (argc >= 3) workloadFile = argv[2];

    // Boot engine
    NanoDBEngine engine(dataDir, DEFAULT_POOL_SIZE);
    Logger* logger = engine.getLogger();

    // Ensure data exists
    DataGenerator gen(logger);
    gen.generate(&engine);

    // Run all 7 required demo test cases first
    BenchmarkRunner runner(&engine, logger);
    logger->separator("DEMO TEST CASES (A-G)");
    runner.runAll();

    // Now process workload file
    logger->separator("WORKLOAD FILE EXECUTION");
    logger->log("Reading workload file: %s", workloadFile);

    FILE* f = fopen(workloadFile, "r");
    if (!f) {
        fprintf(stderr, "[ERROR] Cannot open workload file: %s\n", workloadFile);
        fprintf(stderr, "  Please ensure queries.txt is in the current directory.\n");
        engine.flush();
        return 1;
    }

    char line[MAX_QUERY_LEN];
    int  lineNum  = 0;
    int  executed = 0;
    int  skipped  = 0;
    double totalMs = 0.0;

    while (fgets(line, sizeof(line), f)) {
        lineNum++;
        // Strip trailing whitespace / newlines
        int len = (int)strlen(line);
        while (len > 0 && (line[len-1]=='\n'||line[len-1]=='\r'||line[len-1]==' '))
            line[--len] = '\0';

        // Skip empty lines and comments (#)
        if (len == 0 || line[0] == '#') { skipped++; continue; }

        logger->log("Query [%02d]: %s", lineNum, line);

        double t0 = getTimeMs();

        // Determine priority: lines starting with ADMIN get priority boost
        int prio = PRIORITY_USER;
        if (strncmp(line, "ADMIN", 5) == 0) prio = PRIORITY_ADMIN;

        engine.submitQuery(line, prio);
        engine.drainQueue(false);  // execute, suppress row printing for speed

        double elapsed = getTimeMs() - t0;
        totalMs += elapsed;
        logger->log("  → Executed in %.4f ms", elapsed);
        executed++;
    }
    fclose(f);

    engine.flush();

    logger->separator("WORKLOAD COMPLETE");
    logger->log("Total queries in file  : %d", lineNum);
    logger->log("Queries executed       : %d", executed);
    logger->log("Lines skipped (comment): %d", skipped);
    logger->log("Total execution time   : %.4f ms", totalMs);
    logger->log("Avg time per query     : %.4f ms",
                executed > 0 ? totalMs / executed : 0.0);
    logger->log("Total LRU evictions    : %llu",
                (unsigned long long)engine.getEvictionCount());

    printf("\n══════════════════════════════════════════\n");
    printf("  Workload complete: %d queries executed\n", executed);
    printf("  Total time:        %.4f ms\n", totalMs);
    printf("  Log file:          nanodb_execution.log\n");
    printf("══════════════════════════════════════════\n\n");

    return 0;
}
