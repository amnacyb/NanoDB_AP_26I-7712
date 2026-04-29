#pragma once
#include "../common.h"
#include "Value.h"

// ─── Column Definition ────────────────────────────────────────────────────────
struct Column {
    char    name[MAX_COL_NAME];
    ColType type;
    int     strLen;   // only used when type == STRING (bytes on disk)

    int serializedSize() const {
        if (type == ColType::INT)   return 4;
        if (type == ColType::FLOAT) return 4;
        return strLen;
    }
};

// ─── Schema ───────────────────────────────────────────────────────────────────
struct Schema {
    char   tableName[MAX_TABLE_NAME];
    Column cols[MAX_COLUMNS];
    int    numCols;
    int    pkColIdx;   // primary key column (for AVL index), -1 if none

    Schema() : numCols(0), pkColIdx(-1) {
        tableName[0] = '\0';
    }

    void addColumn(const char* name, ColType type, int strLen = MAX_STRING_STORED) {
        if (numCols >= MAX_COLUMNS) return;
        safe_strcpy(cols[numCols].name, name, MAX_COL_NAME);
        cols[numCols].type   = type;
        cols[numCols].strLen = strLen;
        numCols++;
    }

    int rowSize() const {
        int total = 0;
        for (int i = 0; i < numCols; i++) total += cols[i].serializedSize();
        return total;
    }

    int colIndex(const char* name) const {
        for (int i = 0; i < numCols; i++)
            if (strcmp(cols[i].name, name) == 0) return i;
        return -1;
    }

    int recordsPerPage() const {
        int rs = rowSize();
        return rs > 0 ? PAGE_DATA_SIZE / rs : 0;
    }

    void print() const {
        printf("Schema[%s]: ", tableName);
        for (int i = 0; i < numCols; i++) {
            const char* t = (cols[i].type == ColType::INT)   ? "INT"   :
                            (cols[i].type == ColType::FLOAT) ? "FLOAT" : "STRING";
            printf("%s:%s ", cols[i].name, t);
        }
        printf("\n");
    }
};

// ─── Row ─────────────────────────────────────────────────────────────────────
// Holds an array of Value pointers (polymorphic heterogeneous data).
class Row {
    Value** cols_;
    int     numCols_;

public:
    explicit Row(int n = 0) : numCols_(n) {
        cols_ = (n > 0) ? new Value*[n] : nullptr;
        for (int i = 0; i < n; i++) cols_[i] = nullptr;
    }

    // Deep copy
    Row(const Row& o) : numCols_(o.numCols_) {
        cols_ = (numCols_ > 0) ? new Value*[numCols_] : nullptr;
        for (int i = 0; i < numCols_; i++)
            cols_[i] = o.cols_[i] ? o.cols_[i]->clone() : nullptr;
    }

    Row& operator=(const Row& o) {
        if (this == &o) return *this;
        freeColumns();
        numCols_ = o.numCols_;
        cols_ = (numCols_ > 0) ? new Value*[numCols_] : nullptr;
        for (int i = 0; i < numCols_; i++)
            cols_[i] = o.cols_[i] ? o.cols_[i]->clone() : nullptr;
        return *this;
    }

    ~Row() { freeColumns(); }

    void freeColumns() {
        if (cols_) {
            for (int i = 0; i < numCols_; i++) {
                delete cols_[i];
                cols_[i] = nullptr;
            }
            delete[] cols_;
            cols_ = nullptr;
        }
        numCols_ = 0;
    }

    void setColumn(int i, Value* v) {
        if (i < 0 || i >= numCols_) return;
        delete cols_[i];
        cols_[i] = v;
    }

    Value* getColumn(int i) const {
        if (i < 0 || i >= numCols_) return nullptr;
        return cols_[i];
    }

    int getNumCols() const { return numCols_; }

    // ── Serialize row into binary buffer (schema-driven) ──────────────────────
    void serialize(char* buf, const Schema& s) const {
        char* ptr = buf;
        for (int i = 0; i < s.numCols && i < numCols_; i++) {
            if (cols_[i])
                cols_[i]->serialize(ptr);
            else
                memset(ptr, 0, s.cols[i].serializedSize());
            ptr += s.cols[i].serializedSize();
        }
    }

    // ── Deserialize row from binary buffer (schema-driven) ───────────────────
    void deserialize(const char* buf, const Schema& s) {
        freeColumns();
        numCols_ = s.numCols;
        cols_ = new Value*[numCols_];
        const char* ptr = buf;
        for (int i = 0; i < s.numCols; i++) {
            ColType ct = s.cols[i].type;
            if (ct == ColType::INT) {
                cols_[i] = new IntValue();
            } else if (ct == ColType::FLOAT) {
                cols_[i] = new FloatValue();
            } else {
                cols_[i] = new StringValue("", s.cols[i].strLen);
            }
            cols_[i]->deserialize(ptr);
            ptr += s.cols[i].serializedSize();
        }
    }

    void print(const Schema& s) const {
        for (int i = 0; i < s.numCols && i < numCols_; i++) {
            if (i) printf("|");
            if (cols_[i]) cols_[i]->print();
            else          printf("NULL");
        }
        printf("\n");
    }
};
