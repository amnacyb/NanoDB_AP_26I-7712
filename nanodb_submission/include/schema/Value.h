#pragma once
#include "../common.h"
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cmath>

// ─── Abstract Value ───────────────────────────────────────────────────────────
// Base class for all column values. Enables heterogeneous row storage via
// virtual dispatch (polymorphism) and operator overloading.
class Value {
public:
    virtual ~Value() {}

    virtual ValType getType() const = 0;
    virtual Value*  clone()   const = 0;

    // Comparison operators (operator overloading)
    virtual bool operator>(const Value& o)  const = 0;
    virtual bool operator<(const Value& o)  const = 0;
    virtual bool operator==(const Value& o) const = 0;
    virtual bool operator!=(const Value& o) const { return !(*this == o); }
    virtual bool operator>=(const Value& o) const { return !(*this < o);  }
    virtual bool operator<=(const Value& o) const { return !(*this > o);  }

    // Arithmetic — return new heap-allocated Value; caller owns it.
    virtual Value* add(const Value& o) const = 0;
    virtual Value* sub(const Value& o) const = 0;
    virtual Value* mul(const Value& o) const = 0;
    virtual Value* div(const Value& o) const = 0;
    virtual Value* mod(const Value& o) const = 0;

    // Conversions
    virtual int         toInt()   const = 0;
    virtual float       toFloat() const = 0;
    virtual void        toString(char* buf, int sz) const = 0;

    // Fixed-width binary serialization (schema-driven)
    virtual void serialize(char* buf)         const = 0;
    virtual void deserialize(const char* buf)       = 0;
    virtual int  serializedSize()             const = 0;

    virtual void print() const = 0;
};

// ─── IntValue ─────────────────────────────────────────────────────────────────
class IntValue : public Value {
    int32_t v;
public:
    explicit IntValue(int32_t x = 0) : v(x) {}
    ValType getType() const override { return ValType::INT; }
    Value*  clone()   const override { return new IntValue(v); }

    bool operator>(const Value& o)  const override { return v >  static_cast<const IntValue&>(o).v; }
    bool operator<(const Value& o)  const override { return v <  static_cast<const IntValue&>(o).v; }
    bool operator==(const Value& o) const override { return v == static_cast<const IntValue&>(o).v; }

    Value* add(const Value& o) const override { return new IntValue(v + o.toInt()); }
    Value* sub(const Value& o) const override { return new IntValue(v - o.toInt()); }
    Value* mul(const Value& o) const override { return new IntValue(v * o.toInt()); }
    Value* div(const Value& o) const override {
        int d = o.toInt(); return new IntValue(d ? v / d : 0);
    }
    Value* mod(const Value& o) const override {
        int d = o.toInt(); return new IntValue(d ? v % d : 0);
    }

    int   toInt()   const override { return v; }
    float toFloat() const override { return (float)v; }
    void  toString(char* buf, int sz) const override { snprintf(buf, sz, "%d", v); }

    void serialize(char* buf) const override {
        int32_t net = v;
        buf[0] = (net)       & 0xFF;
        buf[1] = (net >> 8)  & 0xFF;
        buf[2] = (net >> 16) & 0xFF;
        buf[3] = (net >> 24) & 0xFF;
    }
    void deserialize(const char* buf) override {
        v = (int32_t)((unsigned char)buf[0]
                    | ((unsigned char)buf[1] << 8)
                    | ((unsigned char)buf[2] << 16)
                    | ((unsigned char)buf[3] << 24));
    }
    int serializedSize() const override { return 4; }
    void print() const override { printf("%d", v); }
};

// ─── FloatValue ───────────────────────────────────────────────────────────────
class FloatValue : public Value {
    float v;
public:
    explicit FloatValue(float x = 0.f) : v(x) {}
    ValType getType() const override { return ValType::FLOAT; }
    Value*  clone()   const override { return new FloatValue(v); }

    bool operator>(const Value& o)  const override { return v >  o.toFloat(); }
    bool operator<(const Value& o)  const override { return v <  o.toFloat(); }
    bool operator==(const Value& o) const override { return fabsf(v - o.toFloat()) < 1e-6f; }

    Value* add(const Value& o) const override { return new FloatValue(v + o.toFloat()); }
    Value* sub(const Value& o) const override { return new FloatValue(v - o.toFloat()); }
    Value* mul(const Value& o) const override { return new FloatValue(v * o.toFloat()); }
    Value* div(const Value& o) const override {
        float d = o.toFloat(); return new FloatValue(d != 0.f ? v / d : 0.f);
    }
    Value* mod(const Value& o) const override {
        float d = o.toFloat(); return new FloatValue(d != 0.f ? fmodf(v, d) : 0.f);
    }

    int   toInt()   const override { return (int)v; }
    float toFloat() const override { return v; }
    void  toString(char* buf, int sz) const override { snprintf(buf, sz, "%.2f", v); }

    void serialize(char* buf) const override {
        // Store float as 4 bytes (IEEE 754 little-endian)
        uint32_t u; memcpy(&u, &v, 4);
        buf[0] = u & 0xFF; buf[1] = (u>>8)&0xFF;
        buf[2] = (u>>16)&0xFF; buf[3] = (u>>24)&0xFF;
    }
    void deserialize(const char* buf) override {
        uint32_t u = (unsigned char)buf[0]
                   | ((unsigned char)buf[1] << 8)
                   | ((unsigned char)buf[2] << 16)
                   | ((unsigned char)buf[3] << 24);
        memcpy(&v, &u, 4);
    }
    int serializedSize() const override { return 4; }
    void print() const override { printf("%.2f", v); }
};

// ─── StringValue ─────────────────────────────────────────────────────────────
class StringValue : public Value {
    char v[MAX_STRING_STORED + 1];   // null-terminated, fixed capacity
    int  maxLen;
public:
    explicit StringValue(const char* s = "", int maxL = MAX_STRING_STORED)
        : maxLen(maxL)
    {
        v[0] = '\0';
        if (s) strncpy(v, s, maxLen);
        v[maxLen] = '\0';
    }

    ValType getType() const override { return ValType::STRING; }
    Value*  clone()   const override { return new StringValue(v, maxLen); }

    bool operator>(const Value& o)  const override { return strcmp(v, asStr(o)) >  0; }
    bool operator<(const Value& o)  const override { return strcmp(v, asStr(o)) <  0; }
    bool operator==(const Value& o) const override { return strcmp(v, asStr(o)) == 0; }

    // String arithmetic — concat for add, otherwise unsupported (return copy)
    Value* add(const Value& o) const override {
        char buf[MAX_STRING_STORED + 1];
        o.toString(buf, sizeof(buf));
        char res[MAX_STRING_STORED + 1];
        int n = (int)strlen(v);
        strncpy(res, v, n); res[n] = '\0';
        strncat(res, buf, maxLen - n);
        return new StringValue(res, maxLen);
    }
    Value* sub(const Value& o) const override { return clone(); }
    Value* mul(const Value& o) const override { return clone(); }
    Value* div(const Value& o) const override { return clone(); }
    Value* mod(const Value& o) const override { return clone(); }

    int   toInt()   const override { return atoi(v); }
    float toFloat() const override { return (float)atof(v); }
    void  toString(char* buf, int sz) const override { strncpy(buf, v, sz-1); buf[sz-1]='\0'; }

    void serialize(char* buf) const override {
        // Fixed-width: always writes maxLen bytes (padded with zeros)
        memset(buf, 0, maxLen);
        strncpy(buf, v, maxLen);
    }
    void deserialize(const char* buf) override {
        strncpy(v, buf, maxLen);
        v[maxLen] = '\0';
    }
    int serializedSize() const override { return maxLen; }
    void print() const override { printf("%s", v); }

private:
    static const char* asStr(const Value& o) {
        static char tmp[MAX_STRING_STORED + 1];
        o.toString(tmp, sizeof(tmp));
        return tmp;
    }
};

// ─── Value Factory ────────────────────────────────────────────────────────────
inline Value* makeValue(const char* str, ColType type, int maxLen = MAX_STRING_STORED) {
    if (type == ColType::INT)    return new IntValue(atoi(str));
    if (type == ColType::FLOAT)  return new FloatValue((float)atof(str));
    return new StringValue(str, maxLen);
}

inline Value* makeIntValue(int v)         { return new IntValue(v); }
inline Value* makeFloatValue(float v)     { return new FloatValue(v); }
inline Value* makeStringValue(const char* v, int maxLen = MAX_STRING_STORED) {
    return new StringValue(v, maxLen);
}
