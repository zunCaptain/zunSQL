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

	public static DBInstance Open(String name, int M)
	{
		Database db = null;

		try {
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

	public QueryResult Execute(String statement)
	{
		//瀹氫箟涓�涓狶ist<Relation>锛屽皢parse()杩斿洖鐨凴elation瀵硅薄濉叆
		List<Relation> statements = new ArrayList<Relation>();
		try{
			statements.add(Parser.parse(statement));
		}catch(Exception e)
		{
			System.out.println("Syntax error");
			return null;
		}

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
