package npu.zunsql.sqlparser.parser;

import npu.zunsql.sqlparser.ast.QualifiedName;
import org.jparsec.*;

import static java.util.Arrays.asList;

public final class TerminalParser {

    private static final String[] OPERATORS = {
            "+", "-", "*", "/", "%", ">", "<", "=", ">=", "<=", "<>", ".", ",", "(", ")", "[", "]"
    };

    private static final String[] KEYWORDS = {
            "select", "distinct", "from", "as", "where", "group", "by", "having", "order", "asc", "desc",
            "and", "or", "not", "in", "exists", "between", "is", "null", "like", "escape",
            "inner", "outer", "left", "right", "full", "cross", "join", "on",
            "union", "all", "case", "when", "then", "else", "end",
            "insert", "into", "values", "create", "table", "update", "set", "delete", "drop",
            "int", "double", "varchar", "primary", "key",
            "begin", "transaction", "commit", "rollback",
    };

    private static final Terminals TERMS =
            Terminals.operators(OPERATORS).words(Scanners.IDENTIFIER).caseInsensitiveKeywords(asList(KEYWORDS)).build();

    private static final Parser<?> TOKENIZER = Parsers.or(
            Terminals.DecimalLiteral.TOKENIZER, Terminals.StringLiteral.SINGLE_QUOTE_TOKENIZER,
            TERMS.tokenizer());

    static final Parser<String> NUMBER = Terminals.DecimalLiteral.PARSER;
    static final Parser<String> STRING = Terminals.StringLiteral.PARSER;

    static final Parser<String> NAME =
            Parsers.between(term("["), Terminals.fragment(Tokens.Tag.RESERVED, Tokens.Tag.IDENTIFIER), term("]"))
                    .or(Terminals.Identifier.PARSER);


    static final Parser<QualifiedName> QUALIFIED_NAME =
            NAME.sepBy1(term(".")).map(QualifiedName::new);

    public static <T> T parse(Parser<T> parser, String source) {
        return parser.from(TOKENIZER, Scanners.SQL_DELIMITER).parse(source);
    }

    public static Parser<?> term(String term) {
        return TERMS.token(term);
    }

    public static Parser<?> phrase(String phrase) {
        return TERMS.phrase(phrase.split("\\s"));
    }
}
