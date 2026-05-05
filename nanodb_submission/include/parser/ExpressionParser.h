#pragma once
#include "../common.h"
#include "../ds/Stack.h"
#include "Lexer.h"
#include "../schema/Row.h"
#include "../Logger.h"
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cmath>

// ─── Expression Token (used in postfix queue) ─────────────────────────────────
struct ExprToken {
    enum Kind { OPERAND_COL, OPERAND_INT, OPERAND_FLOAT, OPERAND_STR,
                OP_CMP, OP_ARITH, OP_LOGIC, LPAREN } kind;
    char lexeme[128];
    int      intVal;
    float    floatVal;
    ExprToken() : kind(OPERAND_INT), intVal(0), floatVal(0.f) { lexeme[0]='\0'; }
};

// ─── Shunting-Yard (Infix → Postfix) + Evaluator ─────────────────────────────
class ExpressionParser {
    Logger* logger;

    // Operator precedence (higher = tighter binding)
    int precedence(const char* op) const {
        if (!strcmp(op,"OR"))                        return 1;
        if (!strcmp(op,"AND"))                       return 2;
        if (!strcmp(op,"NOT"))                       return 3;
        if (!strcmp(op,"==")||!strcmp(op,"!=")||
            !strcmp(op,">")||!strcmp(op,"<")||
            !strcmp(op,">=")||!strcmp(op,"<="))      return 4;
        if (!strcmp(op,"+")||!strcmp(op,"-"))        return 5;
        if (!strcmp(op,"*")||!strcmp(op,"/")||
            !strcmp(op,"%"))                         return 6;
        return 0;
    }

    bool isOperator(const TType t) const {
        return t == TType::KW_AND   || t == TType::KW_OR   || t == TType::KW_NOT  ||
               t == TType::OP_GT   || t == TType::OP_LT   || t == TType::OP_GE   ||
               t == TType::OP_LE   || t == TType::OP_EQ   || t == TType::OP_NE   ||
               t == TType::OP_PLUS || t == TType::OP_MINUS|| t == TType::OP_STAR ||
               t == TType::OP_SLASH|| t == TType::OP_PERCENT;
    }

    ExprToken makeOp(const char* op, ExprToken::Kind k) {
        ExprToken t; t.kind = k;
        safe_strcpy(t.lexeme, op, 128);
        return t;
    }

    ExprToken makeColOperand(const char* name) {
        ExprToken t; t.kind = ExprToken::OPERAND_COL;
        safe_strcpy(t.lexeme, name, 128);
        return t;
    }

    ExprToken makeIntOperand(int v) {
        ExprToken t; t.kind = ExprToken::OPERAND_INT; t.intVal = v;
        snprintf(t.lexeme, sizeof(t.lexeme), "%d", v);
        return t;
    }

    ExprToken makeFloatOperand(float v) {
        ExprToken t; t.kind = ExprToken::OPERAND_FLOAT; t.floatVal = v;
        snprintf(t.lexeme, sizeof(t.lexeme), "%.2f", v);
        return t;
    }

    ExprToken makeStrOperand(const char* s) {
        ExprToken t; t.kind = ExprToken::OPERAND_STR;
        safe_strcpy(t.lexeme, s, 128);
        return t;
    }

public:
    explicit ExpressionParser(Logger* lg = nullptr) : logger(lg) {}

    // ── Shunting-Yard algorithm: converts infix token stream → postfix array ──
    // Returns number of tokens placed in postfix[].
    int toPostfix(const char* infixExpr,
                  ExprToken* postfix, int maxOut,
                  char* postfixStr, int postfixStrLen)
    {
        Lexer lex(infixExpr);
        Stack<ExprToken> opStack(64);
        int outCount = 0;
        postfixStr[0] = '\0';

        auto emitOutput = [&](const ExprToken& et) {
            if (outCount < maxOut) {
                postfix[outCount++] = et;
                if (postfixStr[0]) strncat(postfixStr, " ", postfixStrLen - strlen(postfixStr) - 1);
                strncat(postfixStr, et.lexeme, postfixStrLen - strlen(postfixStr) - 1);
            }
        };

        auto emitOp = [&]() {
            emitOutput(opStack.pop());
        };

        while (!lex.atEnd()) {
            Token tok = lex.next();

            if (tok.type == TType::END_OF_INPUT) break;

            // Operands → directly to output
            if (tok.type == TType::LIT_INT) {
                emitOutput(makeIntOperand(tok.intVal));
            } else if (tok.type == TType::LIT_FLOAT) {
                emitOutput(makeFloatOperand(tok.floatVal));
            } else if (tok.type == TType::LIT_STRING) {
                emitOutput(makeStrOperand(tok.lexeme));
            } else if (tok.type == TType::IDENTIFIER) {
                // Could be "col" or "table.col" — peek for dot
                char fullName[128];
                safe_strcpy(fullName, tok.lexeme, 128);
                if (lex.peek().type == TType::DOT) {
                    lex.next(); // consume dot
                    Token col = lex.next();
                    snprintf(fullName, sizeof(fullName), "%s.%s", tok.lexeme, col.lexeme);
                }
                emitOutput(makeColOperand(fullName));

            // Left paren → push to opStack
            } else if (tok.type == TType::LPAREN) {
                ExprToken et; et.kind = ExprToken::LPAREN;
                safe_strcpy(et.lexeme, "(", 128);
                opStack.push(et);

            // Right paren → pop until left paren
            } else if (tok.type == TType::RPAREN) {
                while (!opStack.isEmpty() && opStack.peek().kind != ExprToken::LPAREN)
                    emitOp();
                if (!opStack.isEmpty()) opStack.pop(); // discard LPAREN

            // Operators → Shunting-Yard precedence rules
            } else if (isOperator(tok.type)) {
                ExprToken::Kind k;
                const char* opStr = tok.lexeme;
                if (tok.type == TType::KW_AND || tok.type == TType::KW_OR || tok.type == TType::KW_NOT)
                    k = ExprToken::OP_LOGIC;
                else if (tok.type == TType::OP_GT || tok.type == TType::OP_LT ||
                         tok.type == TType::OP_GE || tok.type == TType::OP_LE ||
                         tok.type == TType::OP_EQ || tok.type == TType::OP_NE)
                    k = ExprToken::OP_CMP;
                else
                    k = ExprToken::OP_ARITH;

                // Map TType to canonical string for logic ops
                if (tok.type == TType::KW_AND) opStr = "AND";
                else if (tok.type == TType::KW_OR)  opStr = "OR";
                else if (tok.type == TType::KW_NOT) opStr = "NOT";

                ExprToken opTok = makeOp(opStr, k);

                while (!opStack.isEmpty() &&
                       opStack.peek().kind != ExprToken::LPAREN &&
                       precedence(opStack.peek().lexeme) >= precedence(opStr))
                {
                    emitOp();
                }
                opStack.push(opTok);
            }
        }

        // Drain remaining operators
        while (!opStack.isEmpty()) {
            if (opStack.peek().kind != ExprToken::LPAREN)
                emitOp();
            else
                opStack.pop();
        }

        if (logger)
            logger->log("Infix \"%s\" converted to Postfix \"%s\"", infixExpr, postfixStr);

        return outCount;
    }

    // ── Evaluate a postfix expression for a given Row ─────────────────────────
    // Returns true/false (boolean filter result).
    // Uses a Stack<Value*>; all intermediate Values are heap-allocated and freed.
    bool evaluate(const ExprToken* postfix, int count,
                  const Row& row, const Schema& schema)
    {
        // We store Value* on a small custom stack
        Value** vstack = new Value*[count + 4];
        int vTop = 0;

        auto vpush = [&](Value* v) { vstack[vTop++] = v; };
        auto vpop  = [&]() -> Value* {
            if (vTop <= 0) return new IntValue(0);
            return vstack[--vTop];
        };
        for (int i = 0; i < count; i++) {
            const ExprToken& et = postfix[i];

            if (et.kind == ExprToken::OPERAND_INT) {
                vpush(new IntValue(et.intVal));

            } else if (et.kind == ExprToken::OPERAND_FLOAT) {
                vpush(new FloatValue(et.floatVal));

            } else if (et.kind == ExprToken::OPERAND_STR) {
                vpush(new StringValue(et.lexeme));

            } else if (et.kind == ExprToken::OPERAND_COL) {
                // Look up column in schema (handle "table.col" or "col")
                const char* colName = et.lexeme;
                const char* dot = strchr(colName, '.');
                if (dot) colName = dot + 1;
                int ci = schema.colIndex(colName);
                Value* v = (ci >= 0 && row.getColumn(ci)) ?
                            row.getColumn(ci)->clone() :
                            new IntValue(0);
                vpush(v);

            } else if (et.kind == ExprToken::OP_ARITH) {
                Value* b = vpop();
                Value* a = vpop();
                Value* res = nullptr;
                if      (!strcmp(et.lexeme,"+")) res = a->add(*b);
                else if (!strcmp(et.lexeme,"-")) res = a->sub(*b);
                else if (!strcmp(et.lexeme,"*")) res = a->mul(*b);
                else if (!strcmp(et.lexeme,"/")) res = a->div(*b);
                else if (!strcmp(et.lexeme,"%")) res = a->mod(*b);
                else res = a->clone();
                delete a; delete b;
                if (!res) res = new IntValue(0);
                vpush(res);

            } else if (et.kind == ExprToken::OP_CMP) {
                Value* b = vpop();
                Value* a = vpop();
                bool result = false;
                // Type-safe comparison: if both numeric, compare as float
                if ((a->getType() == ValType::INT || a->getType() == ValType::FLOAT) &&
                    (b->getType() == ValType::INT || b->getType() == ValType::FLOAT)) {
                    float fa = a->toFloat(), fb = b->toFloat();
                    if      (!strcmp(et.lexeme,">"))  result = fa >  fb;
                    else if (!strcmp(et.lexeme,"<"))  result = fa <  fb;
                    else if (!strcmp(et.lexeme,">=")) result = fa >= fb;
                    else if (!strcmp(et.lexeme,"<=")) result = fa <= fb;
                    else if (!strcmp(et.lexeme,"==")||!strcmp(et.lexeme,"="))
                                                       result = fabsf(fa-fb) < 1e-4f;
                    else if (!strcmp(et.lexeme,"!=")) result = fabsf(fa-fb) >= 1e-4f;
                } else {
                    // String comparison
                    char sa[128], sb[128];
                    a->toString(sa, sizeof(sa));
                    b->toString(sb, sizeof(sb));
                    int cmp = strcmp(sa, sb);
                    if      (!strcmp(et.lexeme,">"))  result = cmp >  0;
                    else if (!strcmp(et.lexeme,"<"))  result = cmp <  0;
                    else if (!strcmp(et.lexeme,">=")) result = cmp >= 0;
                    else if (!strcmp(et.lexeme,"<=")) result = cmp <= 0;
                    else if (!strcmp(et.lexeme,"==")||!strcmp(et.lexeme,"="))
                                                       result = cmp == 0;
                    else if (!strcmp(et.lexeme,"!=")) result = cmp != 0;
                }
                delete a; delete b;
                vpush(new IntValue(result ? 1 : 0));

            } else if (et.kind == ExprToken::OP_LOGIC) {
                if (!strcmp(et.lexeme,"NOT")) {
                    Value* a = vpop();
                    bool r = a->toInt() == 0;
                    delete a;
                    vpush(new IntValue(r ? 1 : 0));
                } else {
                    Value* b = vpop();
                    Value* a = vpop();
                    bool r = false;
                    if      (!strcmp(et.lexeme,"AND")) r = a->toInt() && b->toInt();
                    else if (!strcmp(et.lexeme,"OR"))  r = a->toInt() || b->toInt();
                    delete a; delete b;
                    vpush(new IntValue(r ? 1 : 0));
                }
            }
        }

        // Result is top of stack
        bool finalResult = false;
        if (vTop > 0) {
            finalResult = vstack[0]->toInt() != 0;
        }
        for (int i = 0; i < vTop; i++) delete vstack[i];
        delete[] vstack;
        return finalResult;
    }
};
