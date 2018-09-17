package npu.zunsql.sqlparser.ast;

import npu.zunsql.common.FormatObject;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class QualifiedName extends FormatObject implements Iterable<String> {
    public final List<String> names;

    public QualifiedName(List<String> names) {
        this.names = Collections.unmodifiableList(names);
    }

    public static QualifiedName of(String... names) {
        return new QualifiedName(Arrays.asList(names));
    }

    @Override
    public Iterator<String> iterator() {
        return names.iterator();
    }
}
