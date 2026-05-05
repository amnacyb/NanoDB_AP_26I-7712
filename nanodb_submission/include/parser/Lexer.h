#pragma once
#include "../common.h"
#include <cctype>

// ─── Token Types ─────────────────────────────────────────────────────────────
enum class TType {
    // DML Keywords
    KW_SELECT, KW_FROM, KW_WHERE, KW_INSERT, KW_INTO, KW_VALUES,
    KW_UPDATE, KW_SET,  KW_DELETE, KW_JOIN, KW_ON,
    // Logical operators
    KW_AND, KW_OR, KW_NOT,
    // Comparison
    OP_GT, OP_LT, OP_GE, OP_LE, OP_EQ, OP_NE,
    // Arithmetic
    OP_PLUS, OP_MINUS, OP_STAR, OP_SLASH, OP_PERCENT,
    // Punctuation
    LPAREN, RPAREN, COMMA, SEMICOLON, DOT,
    // Literals
    LIT_INT, LIT_FLOAT, LIT_STRING,
    // Identifier
    IDENTIFIER,
    // Special
    STAR_ALL,   // standalone * meaning "all columns"
    END_OF_INPUT,
    INVALID
};

struct Token {
    TType type;
    char  lexeme[MAX_QUERY_LEN];
    int   intVal;
    float floatVal;

    Token() : type(TType::INVALID), intVal(0), floatVal(0.f) { lexeme[0]='\0'; }
    Token(TType t, const char* l)
        : type(t), intVal(0), floatVal(0.f) { safe_strcpy(lexeme, l, MAX_QUERY_LEN); }
};

// ─── Lexer ────────────────────────────────────────────────────────────────────
class Lexer {
    const char* src;
    int         pos;
    int         len;
    Token       peeked;
    bool        hasPeeked;

    void skipWS() {
        while (pos < len && isspace((unsigned char)src[pos])) pos++;
    }

    Token readString() {
        pos++; // skip opening "
        int start = pos;
        while (pos < len && src[pos] != '"') pos++;
        Token t;
        t.type = TType::LIT_STRING;
        int slen = pos - start;
        if (slen >= MAX_QUERY_LEN) slen = MAX_QUERY_LEN - 1;
        strncpy(t.lexeme, src + start, slen);
        t.lexeme[slen] = '\0';
        if (pos < len) pos++; // skip closing "
        return t;
    }

    Token readNumber() {
        int start = pos;
        bool isFloat = false;
        while (pos < len && (isdigit((unsigned char)src[pos]) || src[pos] == '.')) {
            if (src[pos] == '.') isFloat = true;
            pos++;
        }
        Token t;
        int slen = pos - start;
        if (slen >= MAX_QUERY_LEN) slen = MAX_QUERY_LEN - 1;
        strncpy(t.lexeme, src + start, slen);
        t.lexeme[slen] = '\0';
        if (isFloat) { t.type = TType::LIT_FLOAT; t.floatVal = (float)atof(t.lexeme); }
        else         { t.type = TType::LIT_INT;   t.intVal   = atoi(t.lexeme); }
        return t;
    }

    Token readIdOrKeyword() {
        int start = pos;
        while (pos < len && (isalnum((unsigned char)src[pos]) || src[pos] == '_')) pos++;
        Token t;
        int slen = pos - start;
        if (slen >= MAX_QUERY_LEN) slen = MAX_QUERY_LEN - 1;
        strncpy(t.lexeme, src + start, slen);
        t.lexeme[slen] = '\0';

        // Case-insensitive keyword matching
        char upper[MAX_QUERY_LEN];
        for (int i = 0; i <= slen; i++) upper[i] = toupper((unsigned char)t.lexeme[i]);

        if      (!strcmp(upper, "SELECT"))  t.type = TType::KW_SELECT;
        else if (!strcmp(upper, "FROM"))    t.type = TType::KW_FROM;
        else if (!strcmp(upper, "WHERE"))   t.type = TType::KW_WHERE;
        else if (!strcmp(upper, "INSERT"))  t.type = TType::KW_INSERT;
        else if (!strcmp(upper, "INTO"))    t.type = TType::KW_INTO;
        else if (!strcmp(upper, "VALUES"))  t.type = TType::KW_VALUES;
        else if (!strcmp(upper, "UPDATE"))  t.type = TType::KW_UPDATE;
        else if (!strcmp(upper, "SET"))     t.type = TType::KW_SET;
        else if (!strcmp(upper, "DELETE"))  t.type = TType::KW_DELETE;
        else if (!strcmp(upper, "JOIN"))    t.type = TType::KW_JOIN;
        else if (!strcmp(upper, "ON"))      t.type = TType::KW_ON;
        else if (!strcmp(upper, "AND"))     t.type = TType::KW_AND;
        else if (!strcmp(upper, "OR"))      t.type = TType::KW_OR;
        else if (!strcmp(upper, "NOT"))     t.type = TType::KW_NOT;
        else                               t.type = TType::IDENTIFIER;
        return t;
    }

public:
    Lexer() : src(nullptr), pos(0), len(0), hasPeeked(false) {}
    explicit Lexer(const char* s) : src(s), pos(0), hasPeeked(false) {
        len = (int)strlen(s);
    }

    void reset(const char* s) {
        src = s; pos = 0; len = (int)strlen(s); hasPeeked = false;
    }

    Token next() {
        if (hasPeeked) { hasPeeked = false; return peeked; }
        skipWS();
        if (pos >= len) return Token(TType::END_OF_INPUT, "EOF");

        char c = src[pos];

        // String literal
        if (c == '"') return readString();

        // Number
        if (isdigit((unsigned char)c) || (c == '-' && pos+1 < len && isdigit((unsigned char)src[pos+1]))) {
            return readNumber();
        }

        // Identifiers and keywords
        if (isalpha((unsigned char)c) || c == '_') return readIdOrKeyword();

        // Two-char operators
        if (pos + 1 < len) {
            char nc = src[pos+1];
            if (c == '>' && nc == '=') { pos += 2; return Token(TType::OP_GE, ">="); }
            if (c == '<' && nc == '=') { pos += 2; return Token(TType::OP_LE, "<="); }
            if (c == '!' && nc == '=') { pos += 2; return Token(TType::OP_NE, "!="); }
            if (c == '=' && nc == '=') { pos += 2; return Token(TType::OP_EQ, "=="); }
        }

        // Single-char operators / punctuation
        pos++;
        switch (c) {
            case '>': return Token(TType::OP_GT, ">");
            case '<': return Token(TType::OP_LT, "<");
            case '=': return Token(TType::OP_EQ, "=");
            case '+': return Token(TType::OP_PLUS, "+");
            case '-': return Token(TType::OP_MINUS, "-");
            case '*': return Token(TType::OP_STAR, "*");
            case '/': return Token(TType::OP_SLASH, "/");
            case '%': return Token(TType::OP_PERCENT, "%");
            case '(': return Token(TType::LPAREN, "(");
            case ')': return Token(TType::RPAREN, ")");
            case ',': return Token(TType::COMMA, ",");
            case ';': return Token(TType::SEMICOLON, ";");
            case '.': return Token(TType::DOT, ".");
            default:  { char buf[2] = {c, 0}; return Token(TType::INVALID, buf); }
        }
    }

    Token peek() {
        if (!hasPeeked) { peeked = next(); hasPeeked = true; }
        return peeked;
    }

    bool atEnd() { return peek().type == TType::END_OF_INPUT; }
};
