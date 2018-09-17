package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

public final class BinaryRelationalExpression extends FormatObject implements Expression {
    public final Expression expression;
    public final Op operator;
    public final Relation relation;

    public BinaryRelationalExpression(Expression expression, Op operator, Relation relation) {
        this.expression = expression;
        this.operator = operator;
        this.relation = relation;
    }
}
