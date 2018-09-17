package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

import java.util.Collections;
import java.util.List;

public final class Update extends FormatObject implements Relation {
    public final TableRelation table;
    public final List<Assignment> updates;
    public final Expression where;

    public Update(
            TableRelation table, List<Assignment> updates, Expression where) {
        this.table = table;
        this.updates = Collections.unmodifiableList(updates);
        this.where = where;
    }
}