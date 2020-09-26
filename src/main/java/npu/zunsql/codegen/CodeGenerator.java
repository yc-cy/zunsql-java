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
        Boolean isUserTranscation = false;
        
        
        int iiii=0;
        for (Relation statement : statements) {
        	iiii = iiii + 1;
        	System.out.println(iiii);
            if (statement instanceof Begin) {
                inTransaction = true;
                isUserTranscation = true;
                
                System.out.println(isUserTranscation + "\n" );
                ret.add(new Instruction(OpCode.Transaction, null, null, null));
                continue;
            }
            System.out.println(isUserTranscation + "\n");
            if (statement instanceof Commit) {
                inTransaction = false;
                ret.add(new Instruction(OpCode.Commit, null, null, null));
                continue;
            }
            if (statement instanceof Rollback) {
                inTransaction = false;
                ret.add(new Instruction(OpCode.Rollback, null, null, null));
                continue;
            }
            //if (!inTransaction) {
           //     ret.add(new Instruction(OpCode.Transaction, null, null, null));
           // }
            TYPE_SWITCH:
            {
            	if(!isUserTranscation)
            	{
            		System.out.println(isUserTranscation + "\n");
            		System.out.println("111");
            		ret.add(new Instruction(OpCode.Transaction, null, null, null));
            	}
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
                        else if (expr instanceof FunctionExpression)
                        {
                        	ret.addAll(FuncitonExpression2Instruction(expr));
                        }
                        // Do not support other expression now due to lack of OpCode type.
                    }
                    ret.add(new Instruction(OpCode.EndColSelect, null, null, null));
                    Expression where = ((Select) statement).where;
                    ret.addAll(WhereToInstruction(where));
                    
                    // ��ȡGroup by �ֶ�
                    GroupBy group_by = ((Select) statement).groupBy;
                    ret.addAll(groupBy2Instruction(group_by));
                    
                    // ��ȡOrderby
                    OrderBy order_by = ((Select) statement).orderBy;
                    ret.addAll(orderBy2Instruction(order_by));
                    
                    
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
            if (!isUserTranscation) {
                ret.add(new Instruction(OpCode.Commit, null, null, null));
            }
        }
        return ret;
    }

    /**
     * ���������ʽת��Ϊ�ֽ���ָ��
     * @param ex �������ʽ
     * @return ָ����������
     */
    private static List<Instruction> FuncitonExpression2Instruction(Expression ex)
    {
    	String function_name = ((FunctionExpression)ex).function.names.get(0).toLowerCase();	// ��ú���������(Ĭ��ȫ����Сд)
    	
    	List<Expression> args = ((FunctionExpression)ex).args;	// ��ò���������
    	
    	ArrayList<Instruction> ret = new ArrayList<Instruction>();	// ���ص�ָ����
    	
    	// ��ѯ�������ƣ���֮ת��Ϊ�ֽ���ָ��
    	switch (function_name)
    	{
    	case "sum" : 
    		{
    			if (args.size() == 1)
    			{
    				// �����������������sum�����Ĳ���������Ϊ1��
    				ret.add(new Instruction(OpCode.Sum, args.get(0).toString(), null, null));	// ����sum()������˵��Ĭ��ֻ��һ������
    			}
    			break;
    		}
    		
    	default:
    		break;
    	}
    	
    	return ret;	// ���ؽ��
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

    private static List<Instruction> groupBy2Instruction(GroupBy group_by)
    {
    	List<Instruction> ret = new ArrayList<>();
    	
    	if (group_by != null)
    	{
    		for (Expression ex : group_by.by)
    		{
    			ret.add(new Instruction(OpCode.Group, ex.toString(), null, null));
    		}
    	}
    	return ret;
    }
    
    private static List<Instruction> orderBy2Instruction(OrderBy order_by)
    {
    	List<Instruction> ret = new ArrayList<>();
 
    	if (order_by != null)
    	{
    		for (OrderBy.Item item : order_by.items)
    		{
    			if (item.ascending == false)
    			{
    				// �������Ҫ���ǽ�������Ļ�����ô�ֽ����p2��Ϊdesc����֮��Ϊasc
    				ret.add(new Instruction(OpCode.Order, item.by.toString(), "desc", null));
    			}
    			else
    			{
    				ret.add(new Instruction(OpCode.Order, item.by.toString(), "asc", null));
    			}
    		}
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
