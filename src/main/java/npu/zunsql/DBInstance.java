package npu.zunsql;

import npu.zunsql.codegen.CodeGenerator;
import npu.zunsql.sqlparser.Parser;
import npu.zunsql.sqlparser.ast.Relation;
import npu.zunsql.virenv.Instruction;
import npu.zunsql.virenv.QueryResult;
import npu.zunsql.virenv.VirtualMachine;
import npu.zunsql.treemng.Database;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class DBInstance
{
	private Database db;
	private VirtualMachine vm;

	private DBInstance(Database db)
	{
		this.db = db;
		this.vm = new VirtualMachine(db);
	}

	public static DBInstance Open(String name)
	{
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

	public QueryResult Execute(String statement)
	{
		//定义一个List<Relation>，将parse()返回的Relation对象填入
		List<Relation> statements = new ArrayList<Relation>();
		statements.add(Parser.parse(statement));

		List<Instruction> Ins = CodeGenerator.GenerateByteCode(statements);
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
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
