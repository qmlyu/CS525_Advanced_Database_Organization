#include "dberror.h"
#include "expr.h"
#include "record_mgr.h"
#include "tables.h"
#include "test_helper.h"

// helper macros
#define OP_TRUE(left, right, op, message)		\
		do {							\
			Value *result;					\
			MAKE_VALUE(result, DT_INT, -1);			\
			op(left, right, result);				\
			bool b = result->v.boolV;				\
			free(result);					\
			ASSERT_TRUE(b,message);				\
		} while (0)

#define OP_FALSE(left, right, op, message)		\
		do {							\
			Value *result;					\
			MAKE_VALUE(result, DT_INT, -1);			\
			op(left, right, result);				\
			bool b = result->v.boolV;				\
			free(result);					\
			ASSERT_TRUE(!b,message);				\
		} while (0)

// test methods
static void testValueSerialize (void);
static void testOperators (void);
static void testExpressions (void);

char *testName;

// main method
int
main2 (void)
{
    testName = "";

    testValueSerialize();
    testOperators();
    testExpressions();

    return 0;
}

// ************************************************************ 
void
testValueSerialize (void)
{
    testName = "test value serialization and deserialization";
    Value *v;
    char *c;

    v = stringToValue("i10");
    c = serializeValue(v);
    ASSERT_EQUALS_STRING(c, "10", "create Value 10");
    freeVal(v);
    free(c);

    v = stringToValue("f5.3");
    c = serializeValue(v);
    ASSERT_EQUALS_STRING(c, "5.300000", "create Value 5.3");
    freeVal(v);
    free(c);

    v = stringToValue("sHello World");
    c = serializeValue(v);
    ASSERT_EQUALS_STRING(c, "Hello World", "create Value Hello World");
    freeVal(v);
    free(c);

    v = stringToValue("bt");
    c = serializeValue(v);
    ASSERT_EQUALS_STRING(c, "true", "create Value true");
    freeVal(v);
    free(c);

    v = stringToValue("btrue");
    c = serializeValue(v);
    ASSERT_EQUALS_STRING(c, "true", "create Value true");
    freeVal(v);
    free(c);

    TEST_DONE();
}

// ************************************************************
void
testOperators (void)
{
    Value *result;
    testName = "test value comparison and boolean operators";
    MAKE_VALUE(result, DT_INT, 0);
    Value *v1, *v2;

    // equality
    v1 = stringToValue("i10");
    v2 = stringToValue("i10");
    OP_TRUE(v1, v2, valueEquals, "10 = 10");
    freeVal(v1);
    freeVal(v2);

    v1 = stringToValue("i9");
    v2 = stringToValue("i10");
    OP_FALSE(v1, v2, valueEquals, "9 != 10");
    freeVal(v1);
    freeVal(v2);

    v1 = stringToValue("sHello World");
    v2 = stringToValue("sHello World");
    OP_TRUE(v1,v2, valueEquals, "Hello World = Hello World");
    freeVal(v1);
    freeVal(v2);

    v1 = stringToValue("sHello Worl");
    v2 = stringToValue("sHello World");
    OP_FALSE(v1,v2, valueEquals, "Hello Worl != Hello World");
    freeVal(v1);
    freeVal(v2);

    v1 = stringToValue("sHello Worl");
    v2 = stringToValue("sHello Wor");
    OP_FALSE(v1,v2, valueEquals, "Hello Worl != Hello Wor");
    freeVal(v1);
    freeVal(v2);

    // smaller
    v1 = stringToValue("i3");
    v2 = stringToValue("i10");
    OP_TRUE(v1,v2, valueSmaller, "3 < 10");
    freeVal(v1);
    freeVal(v2);

    v1 = stringToValue("f5.0");
    v2 = stringToValue("f6.5");
    OP_TRUE(v1,v2, valueSmaller, "5.0 < 6.5");
    freeVal(v1);
    freeVal(v2);

    // boolean
    v1 = stringToValue("bt");
    v2 = stringToValue("bt");
    OP_TRUE(v1,v2, boolAnd, "t AND t = t");
    freeVal(v1);
    freeVal(v2);

    v1 = stringToValue("bt");
    v2 = stringToValue("bf");
    OP_FALSE(v1,v2, boolAnd, "t AND f = f");
    freeVal(v1);
    freeVal(v2);

    v1 = stringToValue("bt");
    v2 = stringToValue("bf");
    OP_TRUE(v1,v2, boolOr, "t OR f = t");
    freeVal(v1);
    freeVal(v2);

    v1 = stringToValue("bf");
    v2 = stringToValue("bf");
    OP_FALSE(v1,v2, boolOr, "f OR f = f");
    freeVal(v1);
    freeVal(v2);

    v1 = stringToValue("bf");
    TEST_CHECK(boolNot(v1, result));
    freeVal(v1);

    ASSERT_TRUE(result->v.boolV, "!f = t");

    freeVal(result);
    TEST_DONE();
}

// ************************************************************
void
testExpressions (void)
{
    Expr *op, *l, *r;
    Value *res;
    testName = "test complex expressions";

    Value *v1 = stringToValue("i10");
    MAKE_CONS(l, v1);
    evalExpr(NULL, NULL, l, &res);
    OP_TRUE(v1, res, valueEquals, "Const 10");
    freeVal(res);

    Value *v2 = stringToValue("i20");
    MAKE_CONS(r, v2);
    evalExpr(NULL, NULL, r, &res);
    OP_TRUE(v2, res, valueEquals, "Const 20");
    freeVal(res);

    MAKE_BINOP_EXPR(op, l, r, OP_COMP_SMALLER);
    evalExpr(NULL, NULL, op, &res);
    Value *v3 = stringToValue("bt");
    OP_TRUE(v3, res, valueEquals, "Const 10 < Const 20");
    free(v3);
    freeVal(res);

    Value *v4 = stringToValue("bt");
    MAKE_CONS(l, v4);
    evalExpr(NULL, NULL, l, &res);
    OP_TRUE(v4, res, valueEquals, "Const true");
    freeVal(res);

    r = op;
    MAKE_BINOP_EXPR(op, r, l, OP_BOOL_AND);
    evalExpr(NULL, NULL, op, &res);
    Value *v5 = stringToValue("bt");
    OP_TRUE(v5, res, valueEquals, "(Const 10 < Const 20) AND true");
    freeVal(v5);
    freeVal(res);
    freeExpr(op);

    TEST_DONE();
}
