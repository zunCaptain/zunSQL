package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

public final class UnaryExpression extends FormatObject implements Expression {
    public final Expression operand;
    public final Op operator;

    public UnaryExpression(Op operator, Expression operand) {
        this.operand = operand;
        this.operator = operator;
    }
}
