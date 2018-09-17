package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

import java.util.Collections;
import java.util.List;

public final class Create extends FormatObject implements Relation {
    public final List<Column> columns;
    public final TableRelation table;

    public Create(TableRelation table, List<Column> columns) {
        this.columns = Collections.unmodifiableList(columns);
        this.table = table;
    }
}