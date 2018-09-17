package npu.zunsql.ve;

public enum OpCode{

    Transaction,
    Commit,
    Rollback,

    CreateDB,
    DropDB,

    CreateTable,
    DropTable,

    Insert,
    Delete,
    Select,
    Update,
    Set,

    Add,
    Sub,
    Div,
    Mul,
    And,
    Not,
    Or,
    GT,
    GE,
    LT,
    LE,
    EQ,
    NE,
    Neg,

    AddCol,

    BeginPK,
    AddPK,
    EndPK,

    Operand,
    Operator,

    BeginItem,
    AddItemCol,
    EndItem,

    BeginColSelect,
    AddColSelect,
    EndColSelect,

    BeginFilter,
    EndFilter,

    BeginJoin,
    AddTable,
    EndJoin,

    Execute,

    BeginExpression,
    EndExpression

}