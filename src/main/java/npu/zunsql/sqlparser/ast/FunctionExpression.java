package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class FunctionExpression extends FormatObject implements Expression {
    public final QualifiedName function;
    public final List<Expression> args;

    public FunctionExpression(QualifiedName function, List<Expression> args) {
        this.function = function;
        this.args = Collections.unmodifiableList(args);
    }

    public static FunctionExpression of(QualifiedName function, Expression... args) {
        return new FunctionExpression(function, Arrays.asList(args));
    }
}
