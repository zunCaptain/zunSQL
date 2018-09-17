package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

import java.util.Collections;
import java.util.List;

public final class Select extends FormatObject implements Relation {
    public final boolean distinct;
    public final List<Expression> exprs;
    public final List<TableRelation> from;
    public final Expression where;

    public Select(
            boolean distinct, List<Expression> exprs, List<TableRelation> from, Expression where) {
        this.distinct = distinct;
        this.exprs = Collections.unmodifiableList(exprs);
        this.from = Collections.unmodifiableList(from);
        this.where = where;
    }
}
