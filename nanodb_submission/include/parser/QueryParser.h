#pragma once
#include "../common.h"
#include "Lexer.h"
#include "ExpressionParser.h"
#include "../Logger.h"

// ─── Parsed Query ─────────────────────────────────────────────────────────────
struct ParsedQuery {
    QueryType type;

    // SELECT / DELETE
    char tableName[MAX_TABLE_NAME];
    char whereClause[MAX_QUERY_LEN];    // raw infix WHERE string
    ExprToken postfix[MAX_POSTFIX];     // compiled postfix
    int  postfixLen;
    char postfixStr[MAX_QUERY_LEN];     // human-readable postfix string

    // INSERT
    char insertValues[MAX_COLUMNS][128];
    int  numInsertValues;

    // UPDATE
    char updateCol[MAX_COL_NAME];
    char updateVal[128];

    // JOIN
    char joinTables[MAX_TABLES][MAX_TABLE_NAME];
    int  numJoinTables;
    char joinOnLeft[MAX_COL_NAME];
    char joinOnRight[MAX_COL_NAME];

    // Flags
    bool hasWhere;
    int  priority;
    ScanType scanHint;

    ParsedQuery()
        : type(QueryType::SELECT), postfixLen(0), numInsertValues(0),
          numJoinTables(0), hasWhere(false),
          priority(PRIORITY_USER), scanHint(ScanType::SEQUENTIAL)
    {
        tableName[0] = whereClause[0] = postfixStr[0] = '\0';
        updateCol[0] = updateVal[0] = '\0';
        for (int i = 0; i < MAX_TABLES; i++) joinTables[i][0] = '\0';
    }
};

// ─── Query Parser ─────────────────────────────────────────────────────────────
class QueryParser {
    ExpressionParser exprParser;
    Logger*          logger;

    // Advance past optional semicolon
    void consumeSemicolon(Lexer& lex) {
        if (lex.peek().type == TType::SEMICOLON) lex.next();
    }

    // Collect everything after WHERE until end / semicolon as one string
    void collectWhereClause(Lexer& lex, char* out, int outLen) {
        out[0] = '\0';
        int written = 0;
        int depth = 0;
        while (!lex.atEnd()) {
            Token t = lex.peek();
            if (t.type == TType::SEMICOLON) break;
            t = lex.next();
            if (t.type == TType::LPAREN)  depth++;
            if (t.type == TType::RPAREN)  depth--;
            if (written > 0 && written < outLen - 1) out[written++] = ' ';
            int slen = (int)strlen(t.lexeme);
            int space = outLen - written - 1;
            if (slen > space) slen = space;
            strncpy(out + written, t.lexeme, slen);
            written += slen;
            out[written] = '\0';
        }
    }

public:
    explicit QueryParser(Logger* lg) : exprParser(lg), logger(lg) {}

    // ── Main parse entry ─────────────────────────────────────────────────────
    bool parse(const char* queryStr, ParsedQuery& out) {
        out = ParsedQuery();
        Lexer lex(queryStr);

        Token first = lex.peek();

        // Detect ADMIN prefix for priority
        if (first.type == TType::IDENTIFIER && strcmp(first.lexeme, "ADMIN") == 0) {
            lex.next();
            out.priority = PRIORITY_ADMIN;
            first = lex.peek();
        }

        // SELECT
        if (first.type == TType::KW_SELECT) {
            lex.next();
            out.type = QueryType::SELECT;

            // Optional: SELECT * FROM table   OR   SELECT WHERE ...
            Token next = lex.peek();

            // Check for JOIN: SELECT * FROM t1 JOIN t2 ON ...
            if (next.type == TType::OP_STAR || next.type == TType::IDENTIFIER) {
                if (next.type == TType::OP_STAR) lex.next(); // consume *

                // FROM
                if (lex.peek().type == TType::KW_FROM) {
                    lex.next();
                    Token tblTok = lex.next();
                    safe_strcpy(out.tableName, tblTok.lexeme, MAX_TABLE_NAME);
                    out.numJoinTables = 0;
                    safe_strcpy(out.joinTables[out.numJoinTables++], tblTok.lexeme, MAX_TABLE_NAME);

                    // JOIN chain
                    while (lex.peek().type == TType::KW_JOIN) {
                        lex.next(); // consume JOIN
                        Token jt = lex.next();
                        if (out.numJoinTables < MAX_TABLES)
                            safe_strcpy(out.joinTables[out.numJoinTables++], jt.lexeme, MAX_TABLE_NAME);
                        // ON clause (optional, consume it)
                        if (lex.peek().type == TType::KW_ON) {
                            lex.next();
                            // consume left.col = right.col
                            lex.next(); lex.next(); lex.next(); lex.next(); lex.next();
                        }
                    }
                    if (out.numJoinTables > 1) out.type = QueryType::JOIN_SELECT;
                }
            }

            // WHERE
            if (lex.peek().type == TType::KW_WHERE) {
                lex.next();
                collectWhereClause(lex, out.whereClause, MAX_QUERY_LEN);
                out.hasWhere = true;
                out.postfixLen = exprParser.toPostfix(
                    out.whereClause, out.postfix, MAX_POSTFIX,
                    out.postfixStr, MAX_QUERY_LEN);
            }
            consumeSemicolon(lex);
            return true;
        }

        // INSERT INTO table VALUES (v1, v2, ...)
        if (first.type == TType::KW_INSERT) {
            lex.next();
            out.type = QueryType::INSERT;
            if (lex.peek().type == TType::KW_INTO) lex.next();
            Token tblTok = lex.next();
            safe_strcpy(out.tableName, tblTok.lexeme, MAX_TABLE_NAME);
            if (lex.peek().type == TType::KW_VALUES) lex.next();
            if (lex.peek().type == TType::LPAREN)    lex.next();
            out.numInsertValues = 0;
            while (!lex.atEnd()) {
                Token vt = lex.peek();
                if (vt.type == TType::RPAREN || vt.type == TType::SEMICOLON) break;
                if (vt.type == TType::COMMA) { lex.next(); continue; }
                vt = lex.next();
                if (out.numInsertValues < MAX_COLUMNS)
                    safe_strcpy(out.insertValues[out.numInsertValues++], vt.lexeme, 128);
            }
            consumeSemicolon(lex);
            return true;
        }

        // UPDATE table SET col = val WHERE ...
        if (first.type == TType::KW_UPDATE) {
            lex.next();
            out.type = QueryType::UPDATE;
            out.priority = PRIORITY_ADMIN;  // UPDATE is admin-level
            Token tblTok = lex.next();
            safe_strcpy(out.tableName, tblTok.lexeme, MAX_TABLE_NAME);
            if (lex.peek().type == TType::KW_SET) lex.next();
            Token colTok = lex.next();
            safe_strcpy(out.updateCol, colTok.lexeme, MAX_COL_NAME);
            if (lex.peek().type == TType::OP_EQ) lex.next(); // skip =
            Token valTok = lex.next();
            safe_strcpy(out.updateVal, valTok.lexeme, 128);
            if (lex.peek().type == TType::KW_WHERE) {
                lex.next();
                collectWhereClause(lex, out.whereClause, MAX_QUERY_LEN);
                out.hasWhere = true;
                out.postfixLen = exprParser.toPostfix(
                    out.whereClause, out.postfix, MAX_POSTFIX,
                    out.postfixStr, MAX_QUERY_LEN);
            }
            consumeSemicolon(lex);
            return true;
        }

        // DELETE FROM table WHERE ...
        if (first.type == TType::KW_DELETE) {
            lex.next();
            out.type = QueryType::DELETE;
            if (lex.peek().type == TType::KW_FROM) lex.next();
            Token tblTok = lex.next();
            safe_strcpy(out.tableName, tblTok.lexeme, MAX_TABLE_NAME);
            if (lex.peek().type == TType::KW_WHERE) {
                lex.next();
                collectWhereClause(lex, out.whereClause, MAX_QUERY_LEN);
                out.hasWhere = true;
                out.postfixLen = exprParser.toPostfix(
                    out.whereClause, out.postfix, MAX_POSTFIX,
                    out.postfixStr, MAX_QUERY_LEN);
            }
            consumeSemicolon(lex);
            return true;
        }

        if (logger) logger->warn("QueryParser: unrecognized query: %s", queryStr);
        return false;
    }
};
