#pragma once
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <cassert>
#include <ctime>

// ─── Page / Buffer Configuration ────────────────────────────────────────────
static const int PAGE_SIZE          = 4096;
static const int PAGE_HEADER_SIZE   = 64;
static const int PAGE_DATA_SIZE     = PAGE_SIZE - PAGE_HEADER_SIZE;
static const int DEFAULT_POOL_SIZE  = 256;   // max frames in buffer pool
static const int STRESS_POOL_SIZE   = 50;    // for Test Case D

// ─── Schema Limits ───────────────────────────────────────────────────────────
static const int MAX_TABLE_NAME     = 32;
static const int MAX_COL_NAME       = 32;
static const int MAX_COLUMNS        = 20;
static const int MAX_TABLES         = 16;
static const int MAX_STRING_STORED  = 48;   // bytes reserved on disk per STRING col
static const int MAX_PAGES_PER_TABLE= 100000;
static const int HASH_BUCKETS       = 64;

// ─── Query Limits ────────────────────────────────────────────────────────────
static const int MAX_QUERY_LEN      = 2048;
static const int MAX_TOKENS         = 256;
static const int MAX_POSTFIX        = 256;

// ─── Data Types ──────────────────────────────────────────────────────────────
enum class ValType  { INT, FLOAT, STRING };
enum class ColType  { INT, FLOAT, STRING };
enum class QueryType{ SELECT, INSERT, UPDATE, DELETE, JOIN_SELECT };
enum class ScanType { SEQUENTIAL, INDEXED };

// ─── Query Priority ──────────────────────────────────────────────────────────
static const int PRIORITY_USER      = 1;
static const int PRIORITY_SYSTEM    = 5;
static const int PRIORITY_ADMIN     = 10;

// ─── Utility Macros ──────────────────────────────────────────────────────────
#define SAFE_DELETE(p)      do { delete (p);   (p) = nullptr; } while(0)
#define SAFE_DELETE_ARR(p)  do { delete[] (p); (p) = nullptr; } while(0)

inline void safe_strcpy(char* dst, const char* src, int maxLen) {
    if (!src) { dst[0] = '\0'; return; }
    int i = 0;
    while (i < maxLen - 1 && src[i]) { dst[i] = src[i]; i++; }
    dst[i] = '\0';
}

#include <chrono>

inline double getTimeMs() {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration<double, std::milli>(now.time_since_epoch()).count();
}
