package npu.zunsql.sqlparser.parser;

import npu.zunsql.sqlparser.ast.*;
import org.jparsec.Parser;
import org.jparsec.Parsers;

import java.util.List;

public final class RelationParser {

    private static <T> Parser<List<T>> list(Parser<T> p) {
        return p.sepBy1(TerminalParser.term(","));
    }

    static final Parser<TableRelation> TABLE_NAME = TerminalParser.QUALIFIED_NAME.map(TableRelation::new);
    static final Parser<Relation> TABLE = TerminalParser.QUALIFIED_NAME.map(TableRelation::new);
    static final Parser<Boolean> SELECT_CLAUSE = TerminalParser.term("select").next(TerminalParser.term("distinct").succeeds());
    static final Parser<?> DELETE_CLAUSE = TerminalParser.term("delete").next(TerminalParser.term("from"));
    static final Parser<?> INSERT_CLAUSE = TerminalParser.term("insert").next(TerminalParser.term("into"));
    static final Parser<?> UPDATE_CLAUSE = TerminalParser.term("update");
    static final Parser<?> CREATE_CLAUSE = TerminalParser.term("create").next(TerminalParser.term("table"));
    static final Parser<?> DROP_CLAUSE = TerminalParser.term("drop").next(TerminalParser.term("table"));
    static final Parser<?> BEGIN_CLAUSE = TerminalParser.term("begin").next(TerminalParser.term("transaction"));
    static final Parser<?> COMMIT_CLAUSE = TerminalParser.term("commit");
    static final Parser<?> ROLLBACK_CLAUSE = TerminalParser.term("rollback");

    static final Parser<Boolean> selectClause() {
        return TerminalParser.term("select").next(TerminalParser.term("distinct").succeeds());
    }

    static Parser<List<TableRelation>> fromClause() {
        return TerminalParser.term("from").next(TABLE_NAME.sepBy1(TerminalParser.term(",")));
    }

    static Parser<Expression> whereClause(Parser<Expression> cond) {
        return TerminalParser.term("where").next(cond);
    }

    static Parser<Relation> select(
            Parser<Expression> expr, Parser<Expression> cond) {
        return Parsers.sequence(
                SELECT_CLAUSE, list(expr),
                fromClause(),
                whereClause(cond).optional(null),
                Select::new);
    }

    public static Parser<Relation> select() {
        Parser<Expression> expr = ExpressionParser.expression();
        Parser<Expression> cond = ExpressionParser.condition(Parsers.or(expr, ExpressionParser.STRING));
        Parser<Relation> relation = select(expr, cond);
        return relation;
    }

    static Parser<Relation> delete(
            Parser<Expression> cond) {
        return DELETE_CLAUSE.next(Parsers.sequence(
                TABLE_NAME,
                whereClause(cond).optional(null),
                Delete::new));
    }

    public static Parser<Relation> delete() {
        Parser<Expression> expr = ExpressionParser.expression();
        Parser<Expression> cond = ExpressionParser.condition(Parsers.or(expr, ExpressionParser.STRING));
        Parser<Relation> relation = delete(cond);
        return relation;
    }

    static Parser<List<Expression>> valueClause(Parser<List<Expression>> expr) {
        return TerminalParser.term("values").next(expr);
    }

    static Parser<Relation> insert() {
        return INSERT_CLAUSE.next(Parsers.sequence(
                TABLE_NAME,
                ExpressionParser.paren(list(ExpressionParser.QUALIFIED_NAME)),
                valueClause(ExpressionParser.paren(list(ExpressionParser.VALUE))),
                Insert::new));
    }

    static Parser<List<Assignment>> setClause(Parser<List<Assignment>> expr) {
        return TerminalParser.term("set").next(expr);
    }

    static Parser<Relation> update(Parser<Expression> cond) {
        return UPDATE_CLAUSE.next(Parsers.sequence(
                TABLE_NAME,
                setClause(ExpressionParser.assignment().sepBy(TerminalParser.term(","))),
                whereClause(cond).optional(),
                Update::new));
    }

    public static Parser<Relation> update() {
        Parser<Expression> expr = ExpressionParser.expression();
        Parser<Expression> cond = ExpressionParser.condition(Parsers.or(expr, ExpressionParser.STRING));
        Parser<Relation> relation = update(cond);
        return relation;
    }

    static Parser<Column> columnPair() {
        return Parsers.sequence(
                ExpressionParser.QUALIFIED_NAME,
                ExpressionParser.DATA_TYPE,
                ExpressionParser.PRIMARY_KEY.optional(false),
                Column::new);
    }

    static Parser<Relation> create() {
        return CREATE_CLAUSE.next(Parsers.sequence(
                TABLE_NAME,
                ExpressionParser.paren(list(columnPair())),
                Create::new));
    }

    static Parser<Relation> drop() {
        return DROP_CLAUSE.next(TABLE_NAME.map(Drop::new));
    }

    static Parser<Relation> begin() {
        return BEGIN_CLAUSE.retn(new Begin());
    }

    static Parser<Relation> commit() {
        return COMMIT_CLAUSE.retn(new Commit());
    }

    static Parser<Relation> rollback() {
        return ROLLBACK_CLAUSE.retn(new Rollback());
    }

    public static Parser<Relation> Sql() {
        return Parsers.or(select(),
                insert(),
                delete(),
                update(),
                create(),
                drop(),
                begin(),
                commit(),
                rollback());
    }
}
