package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

import java.util.Collections;
import java.util.List;

public final class Insert extends FormatObject implements Relation {
    public final TableRelation table;
    // QualifiedNameExpression
    public final List<Expression> names;
    public final List<Expression> expressions;

    public Insert(
            TableRelation table, List<Expression> names,
            List<Expression> expressions) {
        this.table = table;
        this.names = Collections.unmodifiableList(names);
        this.expressions = Collections.unmodifiableList(expressions);
    }
}