#pragma once
#include "../common.h"
#include "../Logger.h"

// ─── Graph Edge ──────────────────────────────────────────────────────────────
struct GEdge {
    int   u, v;      // vertex (table) indices
    float cost;      // estimated join cost (product of table sizes)
};

// ─── Union-Find (for Kruskal's cycle detection) ───────────────────────────────
struct UnionFind {
    int parent[MAX_TABLES];
    int rank_[MAX_TABLES];

    void init(int n) {
        for (int i = 0; i < n; i++) { parent[i] = i; rank_[i] = 0; }
    }

    int find(int x) {
        while (parent[x] != x) {
            parent[x] = parent[parent[x]]; // path compression
            x = parent[x];
        }
        return x;
    }

    bool unite(int x, int y) {
        int rx = find(x), ry = find(y);
        if (rx == ry) return false;  // cycle
        if      (rank_[rx] < rank_[ry]) parent[rx] = ry;
        else if (rank_[rx] > rank_[ry]) parent[ry] = rx;
        else { parent[ry] = rx; rank_[rx]++; }
        return true;
    }
};

// ─── Graph + MST ─────────────────────────────────────────────────────────────
class Graph {
    char  tblNames[MAX_TABLES][MAX_TABLE_NAME];
    int   tblSizes[MAX_TABLES];
    int   numTables;

    GEdge edges[MAX_TABLES * MAX_TABLES];
    int   numEdges;

    GEdge mstEdges[MAX_TABLES];
    int   numMSTEdges;

    Logger* logger;

    // Simple insertion sort on edges by cost (arrays small → fine)
    void sortEdges() {
        for (int i = 1; i < numEdges; i++) {
            GEdge key = edges[i];
            int j = i - 1;
            while (j >= 0 && edges[j].cost > key.cost) {
                edges[j+1] = edges[j];
                j--;
            }
            edges[j+1] = key;
        }
    }

public:
    Graph(Logger* lg = nullptr)
        : numTables(0), numEdges(0), numMSTEdges(0), logger(lg) {}

    void reset() { numTables = numEdges = numMSTEdges = 0; }

    int addTable(const char* name, int size) {
        if (numTables >= MAX_TABLES) return -1;
        safe_strcpy(tblNames[numTables], name, MAX_TABLE_NAME);
        tblSizes[numTables] = size;
        return numTables++;
    }

    // Cost estimated as product of table sizes (cross-join estimate)
    void addEdge(int u, int v, float cost = -1.f) {
        if (cost < 0) cost = (float)tblSizes[u] * tblSizes[v];
        edges[numEdges++] = {u, v, cost};
    }

    int tableIndex(const char* name) const {
        for (int i = 0; i < numTables; i++)
            if (strcmp(tblNames[i], name) == 0) return i;
        return -1;
    }

    // Build MST using Kruskal's algorithm.
    // Returns number of MST edges.
    int computeMST() {
        sortEdges();       // O(E log E) — here E is tiny (≤ MAX_TABLES²)
        UnionFind uf;
        uf.init(numTables);
        numMSTEdges = 0;

        if (logger) logger->log("Join Optimizer: Running Kruskal's MST on %d tables, %d edges",
                                numTables, numEdges);

        for (int i = 0; i < numEdges && numMSTEdges < numTables - 1; i++) {
            GEdge& e = edges[i];
            if (uf.unite(e.u, e.v)) {
                mstEdges[numMSTEdges++] = e;
                if (logger)
                    logger->log("  MST edge: %s -> %s (cost=%.0f)",
                                tblNames[e.u], tblNames[e.v], e.cost);
            }
        }
        return numMSTEdges;
    }

    // Get join order from MST (BFS from smallest table)
    // Fills joinOrder[] with table names in optimal execution order.
    int getJoinOrder(char joinOrder[][MAX_TABLE_NAME]) {
        if (numMSTEdges == 0) computeMST();

        // adjacency list for MST (small, use fixed arrays)
        int adj[MAX_TABLES][MAX_TABLES];
        int adjCnt[MAX_TABLES] = {};
        for (int i = 0; i < numMSTEdges; i++) {
            int u = mstEdges[i].u, v = mstEdges[i].v;
            adj[u][adjCnt[u]++] = v;
            adj[v][adjCnt[v]++] = u;
        }

        // Find root = table with smallest size (cheapest to start from)
        int root = 0;
        for (int i = 1; i < numTables; i++)
            if (tblSizes[i] < tblSizes[root]) root = i;

        // BFS
        bool visited[MAX_TABLES] = {};
        int  queue[MAX_TABLES], qHead = 0, qTail = 0;
        queue[qTail++] = root;
        visited[root]  = true;
        int cnt = 0;

        while (qHead < qTail) {
            int u = queue[qHead++];
            safe_strcpy(joinOrder[cnt++], tblNames[u], MAX_TABLE_NAME);
            for (int i = 0; i < adjCnt[u]; i++) {
                int v = adj[u][i];
                if (!visited[v]) { visited[v] = true; queue[qTail++] = v; }
            }
        }

        // Add any isolated tables
        for (int i = 0; i < numTables; i++) {
            if (!visited[i]) safe_strcpy(joinOrder[cnt++], tblNames[i], MAX_TABLE_NAME);
        }

        return cnt;
    }

    void printMST(Logger* lg) const {
        if (!lg) return;
        char path[512] = "";
        for (int i = 0; i < numMSTEdges; i++) {
            if (i) strncat(path, " -> ", sizeof(path) - strlen(path) - 1);
            strncat(path, tblNames[mstEdges[i].u], sizeof(path) - strlen(path) - 1);
        }
        if (numMSTEdges > 0) {
            strncat(path, " -> ", sizeof(path) - strlen(path) - 1);
            strncat(path, tblNames[mstEdges[numMSTEdges-1].v],
                    sizeof(path) - strlen(path) - 1);
        }
        lg->log("Multi-table join routed via MST: %s", path);
    }

    int getNumTables() const { return numTables; }
    const char* getTableName(int i) const { return tblNames[i]; }
    int getTableSize(int i) const { return tblSizes[i]; }
};
