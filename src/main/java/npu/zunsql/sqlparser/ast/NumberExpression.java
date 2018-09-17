package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

public final class NumberExpression extends FormatObject implements Expression {
    public final String number;

    public NumberExpression(String number) {
        this.number = number;
    }

    public String toString() {
        return number;
    }
}
