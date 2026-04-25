# NanoDB — Mini Database Engine

**CS-4002 Applied Programming | MS-CS-Spring-2026**
**Instructor:** Bushra Fatima | **Deadline:** Sunday, 10 May 2026

> A from-scratch mini database system implementing the full architecture of a production RDBMS: buffer pool, LRU cache, AVL-tree indexing, Shunting-Yard query parser, MST join optimizer, and priority-queue query scheduling — all without any STL containers.

---

## GitHub Repository

> **[https://github.com/YOUR_USERNAME/nanodb](https://github.com/YOUR_USERNAME/nanodb)**

*(Replace with your actual repository URL before submission)*

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────┐
│                    NanoDB Engine                      │
│                                                       │
│  ┌─────────────┐    ┌──────────────┐                 │
│  │ Query Parser│    │ Priority Queue│                 │
│  │  (Lexer +   │───▶│  (Max-Heap)  │                 │
│  │  Shunting-  │    └──────┬───────┘                 │
│  │  Yard Stack)│           │                         │
│  └─────────────┘           ▼                         │
│                    ┌──────────────┐                  │
│  ┌─────────────┐   │  Execution   │  ┌────────────┐  │
│  │ System      │──▶│   Engine     │──│  Graph/MST │  │
│  │ Catalog     │   │              │  │  Optimizer │  │
│  │ (HashMap)   │   └──────┬───────┘  └────────────┘  │
│  └─────────────┘          │                          │
│                    ┌──────▼───────┐                  │
│  ┌─────────────┐   │  AVL Index   │                  │
│  │ Buffer Pool │   │  O(log N)    │                  │
│  │  (LRU DLL   │◀──│  Lookups     │                  │
│  │   Eviction) │   └──────────────┘                  │
│  └──────┬──────┘                                     │
│         │                                             │
│  ┌──────▼──────┐                                     │
│  │ Disk Manager│  (Binary page I/O)                  │
│  └─────────────┘                                     │
└──────────────────────────────────────────────────────┘
```

---

## Implemented Data Structures (No STL)

| Component | Structure | Time Complexity |
|-----------|-----------|----------------|
| Buffer Pool eviction | Doubly Linked List + hash map | O(1) evict, O(1) promote |
| Query parsing | Custom Stack (Shunting-Yard) | O(N) |
| Table indexing | AVL Tree (self-balancing) | O(log N) insert/search/delete |
| System Catalog | HashMap (djb2 + chaining) | O(1) amortized lookup |
| Query scheduling | Priority Queue (max-heap) | O(log N) insert/extract |
| Join optimization | Graph + Kruskal's MST | O(E log E) |
| Row storage | Polymorphic Value array | O(1) access |

---

## Prerequisites

- **C++17** compatible compiler (GCC ≥ 7, Clang ≥ 5, MSVC 2019+)
- `make` (Linux/macOS) or CMake ≥ 3.14
- Optional: `valgrind` for memory-leak verification

---

## Build Instructions

### Using Makefile (Linux / macOS — Recommended)

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/nanodb.git
cd nanodb

# Build both executables
make all

# Verify build
ls -la nanodb test_runner
```

### Using CMake (cross-platform)

```bash
mkdir build && cd build
cmake ..
cmake --build .
# Executables: build/nanodb, build/test_runner
```

---

## Running the Automated Test Runner

```bash
# This is the main demo command the evaluator will run
./test_runner data queries.txt
```

This will:
1. Boot the NanoDB engine and initialize TPC-H schemas
2. Generate 100,000 TPC-H records (20K customers, 30K orders, 50K lineitems) if not present
3. Execute all 7 required demo test cases (A–G)
4. Process all 50 queries in `queries.txt`
5. Write `nanodb_execution.log` with full execution trace

---

## Running Individual Test Cases

```bash
./nanodb data A    # Test Case A: Parser & Postfix evaluator
./nanodb data B    # Test Case B: Sequential vs AVL Index scan
./nanodb data C    # Test Case C: 3-table JOIN with MST optimizer
./nanodb data D    # Test Case D: Memory stress / LRU eviction
./nanodb data E    # Test Case E: Priority Queue concurrency
./nanodb data F    # Test Case F: Deep nested expression tree
./nanodb data G    # Test Case G: Durability & persistence
./nanodb data all  # All test cases
```

---

## Interactive SQL Shell

```bash
./nanodb data interactive
```

Example queries:
```sql
NanoDB> SELECT * FROM customer WHERE c_custkey == 1
NanoDB> SELECT * FROM customer WHERE c_acctbal > 5000 AND c_mktsegment == "BUILDING"
NanoDB> SELECT * FROM customer JOIN orders ON customer.c_custkey == orders.o_custkey JOIN lineitem ON orders.o_orderkey == lineitem.l_orderkey
NanoDB> INSERT INTO customer VALUES (999999, "Test", "Addr", 5, "555-1234", 1234.56, "BUILDING", "test")
NanoDB> ADMIN UPDATE customer SET c_acctbal = 9999.99 WHERE c_custkey == 1
NanoDB> exit
```

---

## Running Memory Leak Check

```bash
make valgrind
# or
valgrind --leak-check=full --show-leak-kinds=all ./test_runner data queries.txt
```

---

## TPC-H Dataset

The engine **auto-generates** a synthetic TPC-H-compliant dataset on first run:

| Table | Records | Primary Key |
|-------|---------|-------------|
| customer | 20,000 | c_custkey |
| orders | 30,000 | o_orderkey |
| lineitem | 50,000 | l_orderkey |
| **Total** | **100,000** | — |

Data is persisted as binary `.dat` files in the `data/` directory.

---

## Execution Log Format

The `nanodb_execution.log` file contains structured log entries:

```
[LOG] BufferPool initialized: 256 frames (1024 KB)
[LOG] Page 42 evicted via LRU from pool frame 5, written to disk (table: lineitem)
[LOG] Infix "c_acctbal > 5000" converted to Postfix "c_acctbal 5000 >"
[LOG] Multi-table join routed via MST: customer -> orders -> lineitem
[LOG] AVL Index scan time: 0.0012 ms  (result: FOUND)
[LOG] Sequential scan time: 45.2341 ms  (result: FOUND)
```

---

## Complexity Analysis (Summary)

### Buffer Pool (LRU Cache)
- **Insert/Evict**: O(1) — DLL node pointer directly referenced
- **Promote (cache hit)**: O(1) — `moveToFront` on DLL
- **Space**: O(P) where P = pool size

### AVL Index
- **Insert/Delete/Search**: O(log N) — guaranteed by self-balancing
- **Height**: ≤ 1.44 × log₂(N)
- **Space**: O(N)

### Query Parser (Shunting-Yard)
- **Infix → Postfix**: O(T) where T = number of tokens
- **Expression Evaluation**: O(T)
- **Space**: O(T) for stack

### System Catalog (HashMap)
- **Insert/Lookup**: O(1) amortized (djb2 + chaining)
- **Worst case**: O(K) where K = collision chain length

### Join Optimizer (Kruskal's MST)
- **Sort edges**: O(E log E), E = pairs of tables ≤ MAX_TABLES²
- **Union-Find**: O(α(N)) ≈ O(1) amortized

---

## Project Structure

```
nanodb/
├── main.cpp                    # Engine entry point
├── test_runner.cpp             # Automated test runner
├── queries.txt                 # 50-query workload file
├── Makefile
├── CMakeLists.txt
├── .gitignore
├── README.md
├── data/                       # Auto-generated binary data files
└── include/
    ├── common.h                # Constants, enums, utilities
    ├── Logger.h                # Logging system
    ├── ds/
    │   ├── Stack.h             # Custom stack (no STL)
    │   ├── DoublyLinkedList.h  # DLL for LRU cache
    │   ├── HashMap.h           # Custom hash map (chaining)
    │   ├── AVLTree.h           # Self-balancing AVL tree
    │   ├── PriorityQueue.h     # Max-heap priority queue
    │   └── Graph.h             # Graph + Kruskal's MST
    ├── memory/
    │   ├── Page.h              # 4KB raw memory page
    │   ├── DiskManager.h       # Binary file I/O
    │   └── BufferPool.h        # Buffer pool + LRU eviction
    ├── schema/
    │   ├── Value.h             # Polymorphic value types (Int/Float/String)
    │   ├── Row.h               # Heterogeneous row (Value* array)
    │   ├── Table.h             # Table storage + AVL index
    │   └── SystemCatalog.h     # HashMap-based metadata store
    ├── parser/
    │   ├── Lexer.h             # SQL tokenizer
    │   ├── ExpressionParser.h  # Shunting-Yard + postfix evaluator
    │   └── QueryParser.h       # Full SQL-like query parser
    └── engine/
        ├── NanoDBEngine.h      # Central engine controller
        ├── DataGenerator.h     # TPC-H synthetic data generator
        └── BenchmarkRunner.h   # Test cases A-G + empirical benchmarks
```

---

## Academic Integrity

This project is submitted as original work. All data structures (Stack, DoublyLinkedList, HashMap, AVLTree, PriorityQueue, Graph) are implemented from scratch without any STL containers. The code is ready to be defended in a viva voce examination.
