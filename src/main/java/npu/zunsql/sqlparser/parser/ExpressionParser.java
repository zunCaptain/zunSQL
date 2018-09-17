package npu.zunsql.sqlparser.parser;

import npu.zunsql.sqlparser.ast.*;
import org.jparsec.OperatorTable;
import org.jparsec.Parser;
import org.jparsec.Parsers;

import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

import static npu.zunsql.sqlparser.parser.TerminalParser.phrase;
import static npu.zunsql.sqlparser.parser.TerminalParser.term;

public final class ExpressionParser {

    static final Parser<Expression> NUMBER = TerminalParser.NUMBER.map(NumberExpression::new);

    static final Parser<DataType> DOUBLE_TYPE = term("double").retn(DataType.DOUBLE);
    static final Parser<DataType> INT_TYPE = term("int").retn(DataType.INT);
    static final Parser<DataType> VARCHAR_TYPE = term("varchar").retn(DataType.VARCHAR);
    static final Parser<DataType> DATA_TYPE = Parsers.or(DOUBLE_TYPE, INT_TYPE, VARCHAR_TYPE);

    static final Parser<Boolean> PRIMARY_KEY = term("primary").next(term("key")).retn(true);

    static final Parser<Expression> QUALIFIED_NAME = TerminalParser.QUALIFIED_NAME
            .map(QualifiedNameExpression::new);

    static final Parser<Expression> QUALIFIED_WILDCARD = TerminalParser.QUALIFIED_NAME
            .followedBy(phrase(". *"))
            .map(WildcardExpression::new);

    static final Parser<Expression> WILDCARD =
            term("*").<Expression>retn(new WildcardExpression(QualifiedName.of()))
                    .or(QUALIFIED_WILDCARD);

    static final Parser<Expression> STRING = TerminalParser.STRING.map(StringExpression::new);

    static Parser<Expression> VALUE = Parsers.or(
            arithmetic(NUMBER),
            STRING);

    static Parser<Assignment> assignment() {
        return Parsers.sequence(
                TerminalParser.QUALIFIED_NAME,
                TerminalParser.term("=").next(VALUE),
                Assignment::new);
    }

    static Parser<Expression> functionCall(Parser<Expression> param) {
        return Parsers.sequence(
                TerminalParser.QUALIFIED_NAME, paren(param.sepBy(TerminalParser.term(","))),
                FunctionExpression::new);
    }

    static <T> Parser<T> paren(Parser<T> parser) {
        return parser.between(term("("), term(")"));
    }

    static Parser<Expression> arithmetic(Parser<Expression> atom) {
        Parser.Reference<Expression> reference = Parser.newReference();
        Parser<Expression> operand =
                Parsers.or(paren(reference.lazy()), functionCall(reference.lazy()), atom);
        Parser<Expression> parser = new OperatorTable<Expression>()
                .infixl(binary("+", Op.PLUS), 10)
                .infixl(binary("-", Op.MINUS), 10)
                .infixl(binary("*", Op.MUL), 20)
                .infixl(binary("/", Op.DIV), 20)
                .infixl(binary("%", Op.MOD), 20)
                .prefix(unary("-", Op.NEG), 50)
                .build(operand);
        reference.set(parser);
        return parser;
    }

    static Parser<Expression> expression() {
        Parser.Reference<Expression> reference = Parser.newReference();
        Parser<Expression> atom = Parsers.or(
                NUMBER, WILDCARD, QUALIFIED_NAME);
        Parser<Expression> expression = arithmetic(atom).label("expression");
        reference.set(expression);
        return expression;
    }

    static Parser<Expression> compare(Parser<Expression> expr) {
        return Parsers.or(
                compare(expr, ">", Op.GT), compare(expr, ">=", Op.GE),
                compare(expr, "<", Op.LT), compare(expr, "<=", Op.LE),
                compare(expr, "=", Op.EQ), compare(expr, "<>", Op.NE));
    }

    static Parser<Expression> logical(Parser<Expression> expr) {
        Parser.Reference<Expression> ref = Parser.newReference();
        Parser<Expression> parser = new OperatorTable<Expression>()
                .prefix(unary("not", Op.NOT), 30)
                .infixl(binary("and", Op.AND), 20)
                .infixl(binary("or", Op.OR), 10)
                .build(paren(ref.lazy()).or(expr)).label("logical expression");
        ref.set(parser);
        return parser;
    }

    static Parser<Expression> condition(Parser<Expression> expr) {
        Parser<Expression> atom = compare(expr);
        return logical(atom);
    }

    private static Parser<Expression> compare(
            Parser<Expression> operand, String name, Op op) {
        return Parsers.sequence(
                operand, term(name).retn(op), operand,
                BinaryExpression::new);
    }

    private static Parser<BinaryOperator<Expression>> binary(String name, Op op) {
        return term(name).retn((l, r) -> new BinaryExpression(l, op, r));
    }

    private static Parser<UnaryOperator<Expression>> unary(String name, Op op) {
        return term(name).retn(e -> new UnaryExpression(op, e));
    }
}
