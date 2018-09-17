package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

public final class StringExpression extends FormatObject implements Expression {
    public final String string;

    public StringExpression(String string) {
        this.string = string;
    }

    @Override
    public String toString() {
        return string;
    }
}
