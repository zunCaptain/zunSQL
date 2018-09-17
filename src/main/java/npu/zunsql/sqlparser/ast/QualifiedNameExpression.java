package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

public final class QualifiedNameExpression extends FormatObject implements Expression {
    public final QualifiedName qname;

    public QualifiedNameExpression(QualifiedName qname) {
        this.qname = qname;
    }

    public static QualifiedNameExpression of(String... names) {
        return new QualifiedNameExpression(QualifiedName.of(names));
    }

    @Override
    public String toString() {
        return String.join(".", qname.names);
    }
}
