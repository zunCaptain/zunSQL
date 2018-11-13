package npu.zunsql.codegen;

import npu.zunsql.sqlparser.ast.*;
import npu.zunsql.virenv.Instruction;
import npu.zunsql.virenv.OpCode;

import java.util.ArrayList;
import java.util.List;

public class CodeGenerator {
//	static Boolean inTransaction = false;
    public static final List<Instruction> GenerateByteCode(List<Relation> statements) {
        Integer selectTableName = 0;
        List<Instruction> ret = new ArrayList<>();
        Boolean inTransaction = false;
        for (Relation statement : statements) {
            if (statement instanceof Begin) {
                inTransaction = true;
                ret.add(new Instruction(OpCode.Begin, null, null, null));
                continue;
            }
            if (statement instanceof Commit) {
                inTransaction = false;
                ret.add(new Instruction(OpCode.UserCommit, null, null, null));
                continue;
            }
            if (statement instanceof Rollback) {
                inTransaction = false;
                ret.add(new Instruction(OpCode.Rollback, null, null, null));
                continue;
            }
            if (!inTransaction) {
                ret.add(new Instruction(OpCode.Transaction, null, null, null));
            }
            TYPE_SWITCH:
            {
                if (statement instanceof Select) {
                    selectTableName += 1;
                    ret.add(new Instruction(OpCode.BeginJoin, "", "", ""));
                    for (TableRelation table: ((Select) statement).from) {
                        ret.add(new Instruction(OpCode.AddTable, table.tableName.names.get(0), "", ""));
                    }
                    ret.add(new Instruction(OpCode.EndJoin, selectTableName.toString(), "", ""));
                    ret.add(new Instruction(OpCode.Select, null, null, selectTableName.toString()));
                    ret.add(new Instruction(OpCode.BeginColSelect, null, null, null));
                    List<Expression> exprs = ((Select) statement).exprs;
                    for (Expression expr : exprs) {
                        if (expr instanceof WildcardExpression) {
                            ret.add(new Instruction(OpCode.AddColSelect, "*", null, null));
                        } else if (expr instanceof QualifiedNameExpression) {
                            String name = ((QualifiedNameExpression) expr).qname.names.get(0);
                            ret.add(new Instruction(OpCode.AddColSelect, name, null, null));
                        }
                        // Do not support other expression now due to lack of OpCode type.
                    }
                    ret.add(new Instruction(OpCode.EndColSelect, null, null, null));
                    Expression where = ((Select) statement).where;
                    ret.addAll(WhereToInstruction(where));
                    break TYPE_SWITCH;
                }
                if (statement instanceof Insert) {
                    Insert insert = (Insert) statement;
                    String table = insert.table.tableName.names.get(0);
                    ret.add(new Instruction(OpCode.Insert, null, null, table));
                    ret.add(new Instruction(OpCode.BeginItem, null, null, null));
                    for (int i = 0; i < insert.names.size(); i++) {
                        String name = ((QualifiedNameExpression) insert.names.get(i)).qname.names.get(0);
                        ret.add(new Instruction(OpCode.AddItemCol, name, null, null));
                        ret.addAll(ExpressionToInstruction(insert.expressions.get(i)));
                    }
                    ret.add(new Instruction(OpCode.EndItem, null, null, null));
                    break TYPE_SWITCH;
                }
                if (statement instanceof Create) {
                    Create create = (Create) statement;
                    String table = create.table.tableName.names.get(0);
                    ret.add(new Instruction(OpCode.CreateTable, null, null, table));
                    for (Column col : create.columns) {
                        String name = ((QualifiedNameExpression) col.name).qname.names.get(0);
                        String type = DataTypeToString(col.type);
                        ret.add(new Instruction(OpCode.AddCol, name, type, null));
                    }
                    ret.add(new Instruction(OpCode.BeginPK, null, null, null));
                    for (Column col : create.columns) {
                        if (col.isPrimaryKey) {
                            String name = ((QualifiedNameExpression) col.name).qname.names.get(0);
                            ret.add(new Instruction(OpCode.AddPK, name, null, null));
                        }
                    }
                    ret.add(new Instruction(OpCode.EndPK, null, null, null));
                    break TYPE_SWITCH;
                }
                if (statement instanceof Delete) {
                    Delete delete = (Delete) statement;
                    String table = delete.from.tableName.names.get(0);
                    ret.add(new Instruction(OpCode.Delete, null, null, table));
                    Expression where = delete.where;
                    ret.addAll(WhereToInstruction(where));
                    break TYPE_SWITCH;
                }
                if (statement instanceof Drop) {
                    Drop drop = (Drop) statement;
                    String table = drop.table.tableName.names.get(0);
                    ret.add(new Instruction(OpCode.DropTable, null, null, table));
                    break TYPE_SWITCH;
                }
                if (statement instanceof Update) {
                    Update update = (Update) statement;
                    String table = update.table.tableName.names.get(0);
                    ret.add(new Instruction(OpCode.Update, null, null, table));
                    for (Assignment a : update.updates) {
                        String name = a.name.names.get(0);
                        ret.add(new Instruction(OpCode.Set, name, null, null));
                        ret.addAll(ExpressionToInstruction(a.value));
                    }
                    Expression where = update.where;
                    ret.addAll(WhereToInstruction(where));
                }
            }
            ret.add(new Instruction(OpCode.Execute, null, null, null));
            if (!inTransaction) {
                ret.add(new Instruction(OpCode.Commit, null, null, null));
            }
        }
        return ret;
    }

    private static List<Instruction> ExpressionToInstruction(Expression exp) {
        List<Instruction> ret = new ArrayList<>();
        if (exp != null) {
            ret.add(new Instruction(OpCode.BeginExpression, null, null, null));
            ret.addAll(ExpressionToInstructionsInternal(exp));
            ret.add(new Instruction(OpCode.EndExpression, null, null, null));
        }
        return ret;
    }

    private static List<Instruction> WhereToInstruction(Expression where) {
        List<Instruction> ret = new ArrayList<>();
        if (where != null) {
            ret.add(new Instruction(OpCode.BeginFilter, null, null, null));
            ret.addAll(ExpressionToInstructionsInternal(where));
            ret.add(new Instruction(OpCode.EndFilter, null, null, null));
        }
        return ret;
    }

    // public for test
    public static List<Instruction> ExpressionToInstructionsInternal(Expression expr) {
        List<Instruction> ret = new ArrayList<>();
        if (expr instanceof NumberExpression) {
            ret.add(new Instruction(OpCode.Operand, null, ((NumberExpression) expr).number, null));
            return ret;
        }
        if (expr instanceof StringExpression) {
            ret.add(new Instruction(OpCode.Operand, null, ((StringExpression) expr).string, null));
            return ret;
        }
        if (expr instanceof QualifiedNameExpression) {
            ret.add(new Instruction(OpCode.Operand, ((QualifiedNameExpression) expr).qname.names.get(0), null, null));
            return ret;
        }

        // reverse poland

        if (expr instanceof UnaryExpression) {
            ret.addAll(ExpressionToInstructionsInternal(((UnaryExpression) expr).operand));
            ret.add(OpToInstruction(((UnaryExpression) expr).operator));
            return ret;
        }

        if (expr instanceof BinaryExpression) {
            ret.addAll(ExpressionToInstructionsInternal(((BinaryExpression) expr).left));
            ret.addAll(ExpressionToInstructionsInternal(((BinaryExpression) expr).right));
            ret.add(OpToInstruction(((BinaryExpression) expr).operator));
            return ret;
        }

        return ret;
    }

    private static Instruction OpToInstruction(Op op) {
        return new Instruction(OpCode.Operator, OpToString(op), null, null);
    }

    private static String OpToString(Op op) {
        String ret = null;
        switch (op) {
            case GT:
                ret = "GT";
                break;
            case LT:
                ret = "LT";
                break;
            case GE:
                ret = "GE";
                break;
            case LE:
                ret = "LE";
                break;
            case EQ:
                ret = "EQ";
                break;
            case NE:
                ret = "NE";
                break;
            case MUL:
                ret = "Mul";
                break;
            case DIV:
                ret = "Div";
                break;
            case PLUS:
                ret = "Add";
                break;
            case MINUS:
                ret = "Sub";
                break;
            case NEG:
                ret = "Neg";
                break;
            case NOT:
                ret = "Not";
                break;
            case AND:
                ret = "And";
                break;
            case OR:
                ret = "Or";
                break;
        }
        return ret;
    }

    private static String DataTypeToString(DataType dt) {
        String ret = null;
        switch (dt) {
            case INT:
                ret = "Integer";
                break;
            case DOUBLE:
                ret = "Float";
                break;
            case VARCHAR:
                ret = "String";
        }
        return ret;
    }
}
