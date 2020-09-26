package npu.zunsql.virenv;

public enum OpCode{
	Begin,
	UserCommit,
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

    Order,
    Group,
    Sum,
    
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