#pragma once
#include <cstdio>
#include <cassert>
#include <algorithm> // only for std::max — we override below

// Custom max to avoid including <algorithm> which could tempt STL use.
static inline int avl_max(int a, int b) { return a > b ? a : b; }

// ─── AVL Tree ────────────────────────────────────────────────────────────────
// Key: int  (e.g., c_custkey, o_orderkey)
// Value: struct holding page_id + slot within page.
// Guarantees O(log N) insert, search, delete via self-balancing.

struct AVLValue {
    int page_id;
    int slot;
};

struct AVLNode {
    int      key;
    AVLValue val;
    int      height;
    AVLNode* left;
    AVLNode* right;

    AVLNode(int k, const AVLValue& v)
        : key(k), val(v), height(1), left(nullptr), right(nullptr) {}
};

class AVLTree {
    AVLNode* root;
    int      count;

    int height(AVLNode* n) const { return n ? n->height : 0; }

    int balance(AVLNode* n) const {
        return n ? height(n->left) - height(n->right) : 0;
    }

    void updateHeight(AVLNode* n) {
        if (n) n->height = 1 + avl_max(height(n->left), height(n->right));
    }

    // Right rotation
    AVLNode* rotateRight(AVLNode* y) {
        AVLNode* x  = y->left;
        AVLNode* T2 = x->right;
        x->right = y;
        y->left  = T2;
        updateHeight(y);
        updateHeight(x);
        return x;
    }

    // Left rotation
    AVLNode* rotateLeft(AVLNode* x) {
        AVLNode* y  = x->right;
        AVLNode* T2 = y->left;
        y->left  = x;
        x->right = T2;
        updateHeight(x);
        updateHeight(y);
        return y;
    }

    AVLNode* rebalance(AVLNode* n) {
        updateHeight(n);
        int bf = balance(n);

        // Left heavy
        if (bf > 1) {
            if (balance(n->left) < 0)          // Left-Right case
                n->left = rotateLeft(n->left);
            return rotateRight(n);             // Left-Left case
        }
        // Right heavy
        if (bf < -1) {
            if (balance(n->right) > 0)         // Right-Left case
                n->right = rotateRight(n->right);
            return rotateLeft(n);             // Right-Right case
        }
        return n;  // already balanced
    }

    AVLNode* insert(AVLNode* n, int key, const AVLValue& val) {
        if (!n) { count++; return new AVLNode(key, val); }
        if      (key < n->key) n->left  = insert(n->left,  key, val);
        else if (key > n->key) n->right = insert(n->right, key, val);
        else                   n->val   = val;   // duplicate → update
        return rebalance(n);
    }

    AVLNode* minNode(AVLNode* n) {
        while (n->left) n = n->left;
        return n;
    }

    AVLNode* remove(AVLNode* n, int key, bool& removed) {
        if (!n) return nullptr;
        if      (key < n->key) n->left  = remove(n->left,  key, removed);
        else if (key > n->key) n->right = remove(n->right, key, removed);
        else {
            removed = true;
            if (!n->left || !n->right) {
                AVLNode* child = n->left ? n->left : n->right;
                delete n;
                count--;
                return child;
            }
            AVLNode* succ = minNode(n->right);
            n->key = succ->key;
            n->val = succ->val;
            bool dummy = false;
            n->right = remove(n->right, succ->key, dummy);
        }
        return rebalance(n);
    }

    void freeTree(AVLNode* n) {
        if (!n) return;
        freeTree(n->left);
        freeTree(n->right);
        delete n;
    }

    // Inorder traversal helper (writes keys to buf, max maxN entries)
    int inorder(AVLNode* n, int* keysBuf, AVLValue* valsBuf, int maxN, int idx) const {
        if (!n || idx >= maxN) return idx;
        idx = inorder(n->left,  keysBuf, valsBuf, maxN, idx);
        if (idx < maxN) {
            keysBuf[idx] = n->key;
            valsBuf[idx] = n->val;
            idx++;
        }
        idx = inorder(n->right, keysBuf, valsBuf, maxN, idx);
        return idx;
    }

public:
    AVLTree()  : root(nullptr), count(0) {}
    ~AVLTree() { freeTree(root); }

    // O(log N) insert
    void insert(int key, int page_id, int slot) {
        AVLValue v{ page_id, slot };
        root = insert(root, key, v);
    }

    // O(log N) search – returns nullptr if not found
    AVLValue* search(int key) {
        AVLNode* cur = root;
        while (cur) {
            if      (key < cur->key) cur = cur->left;
            else if (key > cur->key) cur = cur->right;
            else                     return &cur->val;
        }
        return nullptr;
    }

    // O(log N) delete
    bool remove(int key) {
        bool removed = false;
        root = remove(root, key, removed);
        return removed;
    }

    int  size()   const { return count; }
    int  getHeight() const { return height(root); }
    bool empty()  const { return count == 0; }

    // Return sorted keys (for debugging)
    int getSorted(int* keysBuf, AVLValue* valsBuf, int maxN) const {
        return inorder(root, keysBuf, valsBuf, maxN, 0);
    }
};
