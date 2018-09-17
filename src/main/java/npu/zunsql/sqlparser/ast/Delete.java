package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

public final class Delete extends FormatObject implements Relation {
    public final TableRelation from;
    public final Expression where;

    public Delete(
            TableRelation from, Expression where) {
        this.from = from;
        this.where = where;
    }
}