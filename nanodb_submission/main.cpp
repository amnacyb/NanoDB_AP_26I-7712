#include "include/common.h"
#include "include/engine/NanoDBEngine.h"
#include "include/engine/DataGenerator.h"
#include "include/engine/BenchmarkRunner.h"
#include <cstdio>
#include <cstring>

// ─── NanoDB Main ─────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    printf("╔══════════════════════════════════════════════════════════╗\n");
    printf("║          NanoDB — Mini Database Engine v1.0             ║\n");
    printf("║   CS-4002 Applied Programming  |  MS-CS-Spring-2026     ║\n");
    printf("╚══════════════════════════════════════════════════════════╝\n\n");

    const char* dataDir = "data";
    if (argc >= 2) dataDir = argv[1];

    // Boot engine with full-size buffer pool
    NanoDBEngine engine(dataDir, DEFAULT_POOL_SIZE);
    Logger* logger = engine.getLogger();

    // Generate TPC-H data if not already present
    DataGenerator gen(logger);
    gen.generate(&engine);

    BenchmarkRunner runner(&engine, logger);

    // ── Mode selection ────────────────────────────────────────────────────────
    if (argc >= 3) {
        const char* mode = argv[2];
        if (strcmp(mode, "all") == 0) {
            runner.runAll();
        } else if (mode[0] >= 'A' && mode[0] <= 'G') {
            runner.runTestCase(mode[0]);
        } else if (strcmp(mode, "interactive") == 0) {
            // Interactive SQL shell
            logger->log("Entering interactive mode. Type 'exit' to quit.");
            printf("\nNanoDB> ");
            char buf[MAX_QUERY_LEN];
            while (fgets(buf, sizeof(buf), stdin)) {
                // Strip newline
                int len = (int)strlen(buf);
                while (len > 0 && (buf[len-1]=='\n'||buf[len-1]=='\r')) buf[--len]='\0';
                if (strcmp(buf,"exit")==0 || strcmp(buf,"quit")==0) break;
                if (buf[0] == '\0') { printf("NanoDB> "); continue; }
                engine.executeQuery(buf, true);
                printf("NanoDB> ");
            }
        } else {
            printf("Usage: %s [datadir] [all|A|B|C|D|E|F|G|interactive]\n", argv[0]);
        }
    } else {
        // Default: run all test cases
        runner.runAll();
    }

    engine.flush();
    printf("\nNanoDB shutdown complete. Log: nanodb_execution.log\n");
    return 0;
}
