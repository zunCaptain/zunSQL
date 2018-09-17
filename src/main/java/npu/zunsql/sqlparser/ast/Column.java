package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

public final class Column extends FormatObject {
    // QualifiedNameExpression
    public final Expression name;
    public final DataType type;
    public final Boolean isPrimaryKey;

    public Column(Expression name, DataType type, Boolean isPrimaryKey) {
        this.name = name;
        this.type = type;
        this.isPrimaryKey = isPrimaryKey;
    }
}
