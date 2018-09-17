package npu.zunsql.sqlparser;

import npu.zunsql.sqlparser.ast.Relation;
import npu.zunsql.sqlparser.parser.RelationParser;
import npu.zunsql.sqlparser.parser.TerminalParser;

public class Parser {
    public static final Relation parse(String stmt) {
        return TerminalParser.parse(RelationParser.Sql(), stmt);
    }
}
