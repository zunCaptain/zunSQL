package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

public final class TableRelation extends FormatObject implements Relation {
    public final QualifiedName tableName;

    public TableRelation(QualifiedName tableName) {
        this.tableName = tableName;
    }
}
