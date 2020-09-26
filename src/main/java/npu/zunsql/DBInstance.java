package npu.zunsql;

import npu.zunsql.codegen.CodeGenerator;
import npu.zunsql.sqlparser.Parser;
import npu.zunsql.sqlparser.ast.Relation;
import npu.zunsql.virenv.Instruction;
import npu.zunsql.virenv.QueryResult;
import npu.zunsql.virenv.VirtualMachine;
import npu.zunsql.treemng.Database;

import java.io.IOException;
import java.sql.Statement;
import java.util.List;
import java.util.ArrayList;

public class DBInstance
{
	private Database db;
	private VirtualMachine vm;

	//实例初始化赋值
	private DBInstance(Database db)
	{
		this.db = db;
		this.vm = new VirtualMachine(db);
	}

	public static DBInstance Open(String name, int M)
	{
		Database db = null;

		try {
			//建立数据库逻辑实例
			db = new Database(name,M);
		}catch(IOException ie){
			ie.printStackTrace();
			System.exit(-1);
		}catch(ClassNotFoundException ce) {
			ce.printStackTrace();
			System.exit(-1);
		}
		return new DBInstance(db);
	}
	
	public static DBInstance Open(String name) {
		Database db = null;

		try {
			db = new Database(name);
		}catch(IOException ie){
			ie.printStackTrace();
			System.exit(-1);
		}catch(ClassNotFoundException ce) {
			ce.printStackTrace();
			System.exit(-1);
		}
		return new DBInstance(db);
	}

	List<Relation> statements = new ArrayList<Relation>();
	
	public QueryResult Execute(String statement)
	{
		
		try{
			statements.add(Parser.parse(statement));
		}catch(Exception e)
		{
			System.out.println("Syntax error");
			return null;
		}

		//for(int i=0;i<statements.size();i++)
		//    System.out.println(statements.get(i));
		
		List<Instruction> Ins = CodeGenerator.GenerateByteCode(statements);
		
		//for(int i=0;i<Ins.size();i++)
		//    System.out.println(Ins.get(i));
		
		try {
			return vm.run(Ins);
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}

	public void Close()
	{
		try {
			db.close();
			statements.clear();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
