package npu.zunsql.virenv;

import npu.zunsql.treemng.*;

import java.io.IOException;
import java.util.*;

import javax.sound.sampled.Port.Info;

public class VirtualMachine {
	// 作为过滤器来对记录进行筛选
	private List<EvalDiscription> filters;
	// 存储被选出的列
	private List<String> selectedColumns;
	// 存储要插入的记录
	private List<AttrInstance> record;
	// 存储要创建表的各项表头，该数据结构仅用于创建表
	private List<Column> columns;
	// 存储execute指令执行后的查询结构，仅select指令对应的操作会使得该集合非空
	private QueryResult result;
	// 要操作的对象表名
	private String targetTable;
	// 创建表时主键的名称存储在该变量中
	private String pkName;
	// 要更新的属性名称，顺序必须与下一个变量的顺序一致
	private List<String> updateAttrs;
	// 要更新的属性值，顺序必须与上一个变量的顺序一致
	private List<List<EvalDiscription>> updateValues;
	// 临时变量
	private List<EvalDiscription> singleUpdateValue;
	// 记录本次execute将执行的命令
	private Activity activity;
	// 作为join操作的结果集
	private QueryResult joinResult;
	// 事务句柄
	private Transaction tran;

	private boolean isJoin = false;
	private int joinIndex = 0;

	private boolean suvReadOnly;
	private boolean recordReadOnly;
	private boolean columnsReadOnly;
	private boolean selectedColumnsReadOnly;
	private Database db;

	public VirtualMachine(Database pdb) {
		recordReadOnly = true;
		columnsReadOnly = true;
		selectedColumnsReadOnly = true;
		suvReadOnly = true;

		tran = null;
		result = null;
		activity = null;
		targetTable = null;
		joinResult = null;

		filters = new ArrayList<>();
		selectedColumns = new ArrayList<>();
		record = new ArrayList<>();
		columns = new ArrayList<>();
		updateAttrs = new ArrayList<>();
		updateValues = new ArrayList<>();
		singleUpdateValue = new ArrayList<>();

		pkName = null;
		db = pdb;
	}

	public QueryResult run(List<Instruction> instructions) throws Exception {

		for (Instruction cmd : instructions) {
			//System.out.println(cmd.opCode+" "+cmd.p1+" "+cmd.p2+" "+cmd.p3);
			run(cmd);
		}
		// isJoin= false;
		return result;
	}

	private void run(Instruction instruction) throws IOException, ClassNotFoundException {
		OpCode opCode = instruction.opCode;
		String p1 = instruction.p1;
		String p2 = instruction.p2;
		String p3 = instruction.p3;

		// 所有操作都是延时操作，即在execute后生效，其他命令只会向VM中填充信息
		// 特例是commit指令和rollback指令会立即执行
		switch (opCode) {
		// 下面是关于事务的处理代码
		case Transaction:
			// 如果这里不能提供Transaction的类型，那么只能在execute的时候由虚拟机来自动推断
			// 这里不做任何处理，因为上一层并没有交给本层事务类型
			break;

		case Commit:
			try {
				tran.Commit();
			} catch (IOException e) {
				Util.log("提交失败");
				throw e;
			}
			break;

		case Rollback:
			tran.RollBack();
			try {
				db = new Database(db.getDatabaseName());
			} catch (IOException ie) {
				ie.printStackTrace();
				System.exit(-1);
			} catch (ClassNotFoundException ce) {
				ce.printStackTrace();
				System.exit(-1);
			}
			break;

		// 下面是创建表的处理代码
		case CreateTable:
			columns.clear();
			activity = Activity.CreateTable;
			columnsReadOnly = false;
			targetTable = p3;
			break;

		case AddCol:
			columns.add(new Column(p1, p2));
			break;

		case BeginPK:
			// 在只支持一个属性作为主键的条件下，此操作本无意义
			// 但指定主键意味着属性信息输入完毕，因此将columnsReadOnly置为true
			columnsReadOnly = true;
			break;

		case AddPK:
			// 在只支持一个属性作为主键的条件下，直接对pkName赋值即可
			pkName = p1;
			break;

		case EndPK:
			// 在只支持一个属性作为主键的条件下，此操作无意义
			// 暂时将此命令作为createTable结束的标志
			break;

		// 下面是删除表的操作
		case DropTable:
			activity = Activity.DropTable;
			targetTable = p3;
			break;

		// 下面是插入操作，这是个延时操作
		case Insert:
			activity = Activity.Insert;
			targetTable = p3;
			record.clear();
			updateValues.clear();

			break;

		// 下面是删除操作，这是个延时操作
		case Delete:
			activity = Activity.Delete;
			targetTable = p3;
			break;

		// 下面是选择操作，这是个延时操作
		case Select:
			activity = Activity.Select;
			targetTable = p3;

			break;

		// 下面是更新操作，这是个延时操作
		case Update:
			activity = Activity.Update;
			targetTable = p3;
			break;

		// 下面是关于插入一条记录的内容的操作
		case BeginItem:
			recordReadOnly = false;
			break;

		case AddItemCol:
			record.add(new AttrInstance(p1, p2, p3));

		case EndItem:
			recordReadOnly = true;
			break;

		// 关于选择器的选项，这里借助表达式实现，仅在最后将记录的表达式传给filters
		case BeginFilter:
			suvReadOnly = false;
			singleUpdateValue = new ArrayList<>();
			break;

		case EndFilter:
			filters = singleUpdateValue;
			// System.out.println("filters name"+filters.get(0).col_name);
			suvReadOnly = true;
			break;

		// 下面是关于select选择的属性的设置
		case BeginColSelect:
			selectedColumnsReadOnly = false;
			break;

		case AddColSelect:
			selectedColumns.add(p1);
			break;

		case EndColSelect:
			selectedColumnsReadOnly = true;
			break;

		// 下面是处理选择的表的连接操作的代码
		case BeginJoin:
			// 接收到join命令，清空临时表
			joinResult = null;
			isJoin = true;
			joinIndex = 0;
			tran = db.beginReadTrans();
			break;

		case AddTable:
			targetTable = p1;
			// 调用下层方法，加载p1表，将自然连接的结果存入joinResult
			join(targetTable);
			break;

		case EndJoin:
			break;

		// 下面的代码设置update要更新的值，形式为colName=Expression
		case Set:
			updateAttrs.add(p1);
			break;

		case BeginExpression:
			// updateValues.clear();
			suvReadOnly = false;
			singleUpdateValue = new ArrayList<>();
			break;

		case EndExpression:
			// System.out.println("###singleUpdateValue:"+singleUpdateValue.get(0).cmd+" "+
			// singleUpdateValue.get(0).col_name+" "+singleUpdateValue.get(0).constant);
			updateValues.add(singleUpdateValue);
			// System.out.println(updateValues.size());
			// System.out.println("*****updateValue***"+updateValues.get(0).get(0).cmd);
			// System.out.println("*****updateValue***"+updateValues.get(0).get(0).col_name);
			// System.out.println("*****updateValue***"+updateValues.get(0).get(0).constant);
			suvReadOnly = true;
			break;

		// 记录Expression描述的代码
		case Operand:
			singleUpdateValue.add(new EvalDiscription(opCode, p1, p2));
			// System.out.println("###singleUpdateValue:"+singleUpdateValue.get(0).cmd+" "+
			// singleUpdateValue.get(0).col_name+" "+singleUpdateValue.get(0).constant);
			break;

		case Operator:
			singleUpdateValue.add(new EvalDiscription(OpCode.valueOf(p1), null, null));
			break;

		case Execute:
			execute();
			break;

		default:
			Util.log("没有这样的字节码: " + opCode + " " + p1 + " " + p2 + " " + p3);
			break;

		}
	}

	private void ConditonClear() throws IOException, ClassNotFoundException {
		recordReadOnly = true;
		columnsReadOnly = true;
		selectedColumnsReadOnly = true;
		suvReadOnly = true;

		// tran = null;
		// result = null;
		activity = null;
		targetTable = null;
		joinResult = null;
	}

	private void execute() throws IOException, ClassNotFoundException {
		result = new QueryResult();
		switch (activity) {
		case Select:
			select();
			// ConditonClear();
			isJoin = false;
			break;
		case Delete:
			delete();
			break;
		case Update:
			update();
			break;
		case Insert:
			insert();
			updateValues.clear();
			break;
		case CreateTable:
			createTable();
			break;
		case DropTable:
			dropTable();
			break;
		default:
			break;
		}
	}

	private void dropTable() throws IOException, ClassNotFoundException {
		tran = db.beginWriteTrans();
		if (db.dropTable(targetTable, tran) == false) {
			Util.log("删除表失败");
		}
	}

	private void createTable() throws IOException, ClassNotFoundException {
		// 需要开启一个写事务
		tran = db.beginWriteTrans();

		List<String> headerName = new ArrayList<>();
		List<BasicType> headerType = new ArrayList<>();
		for (Column n : columns) {
			// System.out.println("#######name:"+n.ColumnName+"##########");
			headerName.add(n.ColumnName);
			switch (n.getColumnType()) {
			case "String":
				headerType.add(BasicType.String);
				break;
			case "Float":
				headerType.add(BasicType.Float);
				break;
			case "Integer":
				headerType.add(BasicType.Integer);
			}
		}
		// System.out.println("headerName size"+headerName.size());
		// for(int i = 0 ; i < headerName.size();i++)
		// System.out.println(headerName.get(i));
		if (db.createTable(targetTable, pkName, headerName, headerType, tran) == null) {
			Util.log("创建表失败");
		}
	}

	/**
	 * 检查当前记录是否满足where子句的条件
	 *
	 * @param p 当前表上的指针
	 * @return 满足条件返回true，否则返回false
	 */
	private boolean check(Cursor p) throws IOException, ClassNotFoundException {
		// 如果没有where子句，那么返回true，即对所有记录都执行操作
		if (filters.size() == 0) {
			return true;
		}

		UnionOperand ans;
		if (isJoin)
			ans = eval(filters, joinIndex);
		else {
			ans = eval(filters, p);
			// System.out.println("this should show twice");
		}
		if (ans.getType() == BasicType.String) {
			Util.log("where子句的表达式返回值不能为String");
			return false;
		} else if (Math.abs(Double.valueOf(ans.getValue())) < 1e-10) {
			return false;
		} else {
			return true;
		}
	}

	//根据joinIndex检测该条记录是否满足filter
	private boolean check(int Index) throws IOException, ClassNotFoundException {
		// 如果没有where子句，那么返回true，即对所有记录都执行操作
		if (filters.size() == 0) {
			return true;
		}

		UnionOperand ans;
		ans = eval(filters, Index);
		if (ans.getType() == BasicType.String) {
			Util.log("where子句的表达式返回值不能为String");
			return false;
		} else if (Math.abs(Double.valueOf(ans.getValue())) < 1e-10) {
			return false;
		} else {
			return true;
		}
	}
	
	private void select() throws IOException, ClassNotFoundException {

		// 构造结果集的表头
		List<Column> selected = new ArrayList<>();
		List<String> temp;
		for (String colName : selectedColumns) {
			Column col = new Column(colName);
			selected.add(col);
		}
		result = new QueryResult(selected);

		if (isJoin) {

			if (selected.get(0).getColumnName().equals("*")) {
				for(int indexi = 0 ; indexi < joinResult.getRes().size(); ++ indexi)
				{
					if(check(indexi))
					{
						result.addRecord(joinResult.getRes().get(indexi));
						result.addAffectedCount();
					}
					
				}
				return ;
				
			}
			temp = joinResult.getHeaderString();

			// 用于joinResult的循环匹配。
			for (int k = 0; k < joinResult.getRes().size(); k++, joinIndex++) {
				//此处应该检测joinResult.get(k)是否满足filter
				if(check(k)){
				List<String> ansRecord = new ArrayList<>();
				for (int i = 0; i < temp.size(); i++) {
					for (int j = 0; j < selected.size(); j++) {
						if (selected.get(j).getColumnName().equals(temp.get(i))) {
							
							ansRecord.add(joinResult.getRes().get(k).get(i));
							result.addAffectedCount();
						}
					}
				}
				result.addRecord(ansRecord);
				}
			}

		} else {
			Cursor p;
			try {
				p = db.getTable(targetTable, tran).createCursor(tran);
			} catch (Exception e) {
				throw e;
			}
			temp = joinResult.getHeaderString();
			while (p != null) {
				if (check(p)) {
					List<String> ansRecord = new ArrayList<>();
					for (int i = 0; i < temp.size(); i++) {
						for (int j = 0; j < selected.size(); j++) {
							if (selected.get(j).getColumnName().equals(temp.get(i))) {
								ansRecord.add(p.getData().get(i));
							}
						}
					}
					result.addRecord(ansRecord);
				}
				p.moveToNext(tran);
			}
		}
	}

	private void delete() throws IOException, ClassNotFoundException {
		tran = db.beginWriteTrans();

//        //因下层未提供接口，暂时注释掉
//        if(filters.size()==0){
//            db.getTable(targetTable,tran).clear(tran);
//        }

		Cursor p = db.getTable(targetTable, tran).createCursor(tran);
		
		while (!p.isEmpty()) {
			if (check(p)) {
				if (p.delete(tran)) {
					result.addAffectedCount();
				}
			} else {
				if (false == p.moveToNext(tran)) {
					p = null;
				}
			}
		}
	}

	/**
	 * 对全表进行更新
	 */
	private void update() throws IOException, ClassNotFoundException {
		tran = db.beginWriteTrans();
		Cursor p = db.getTable(targetTable, tran).createCursor(tran);
		List<String> header = db.getTable(targetTable, tran).getColumnsName();
		// for(int i = 0 ; i < header.size() ; i++)
		// System.out.println("colnumnsname:"+header.get(i));
		while (p != null) {
			List<String> row = p.getData();
			// System.out.println(check(p));
			if (check(p)) {
				// System.out.println(updateAttrs.get(0));
				// System.out.println("updateAttrs.size:"+updateAttrs.size());

				for (int i = 0; i < updateAttrs.size(); i++) {
					// 查询要更新的属性的信息并创建cell对象来执行更新
					// String attrname = record.get(i).attrName;
					String attrname = updateAttrs.get(i);
					// String attrname = "name";
					// System.out.println("record
					// name:"+attrname+",updateAttrs.size="+updateAttrs.size());
					// 循环的方式是否正确?
					/*
					 * for (String info : header) { if (info.equals(attrname)) {
					 * System.out.println("******"+info); row.set(i, eval(updateValues.get(i),
					 * p).getValue());
					 * System.out.println("set value:"+i+" "+eval(updateValues.get(i),
					 * p).getValue()); } }
					 */
					// System.out.println("##################");
					/*
					 * for(int l = 0 ; l < updateValues.size() ;l++) { for(int k = 0 ; k <
					 * updateValues.size();k++) {
					 * 
					 * System.out.println(updateValues.get(l).get(k)); } }
					 */
					// System.out.println("##################");
					// ;
					for (int j = 0; j < header.size(); j++) {

						if (header.get(j).equals(attrname)) {
							// System.out.println("####"+header.get(j));
							row.set(j, eval(updateValues.get(i), p).getValue());
							// System.out.println("set value:"+j+" "+updateValues.get(i).get(0).cmd);
							// for(int ii =0 ; ii < updateValues.get(0).size() ; ii++){
							// System.out.println("updataValue:"+updateValues.get(0).get(ii).cmd+"
							// "+updateValues.get(0).get(ii).col_name
							// +" "+updateValues.get(0).get(ii).constant);
							// }
						}
					}
				}
			}
			if (p.setData(tran, row)) {
				// for(String info:row)
				// System.out.println("row information: "+info);
				result.addAffectedCount();
			}
			if (false == p.moveToNext(tran)) {
				p = null;
			}
		}
	}

	/**
	 * 将一条记录插入到表中 因为上层没有产生default，下层也未提供接口，因此这里每次只能插入一条完整的记录
	 */
	private void insert() throws IOException, ClassNotFoundException {
		tran = db.beginWriteTrans();
		List<String> colValues = new ArrayList<>();

		for (List<EvalDiscription> item : updateValues) {
			colValues.add(eval(item, null).getValue());
		}

		if (db.getTable(targetTable, tran).createCursor(tran).insert(tran, colValues)) {
			result.addAffectedCount();
		}
	}

	/**
	 * 确定一个字符串值的最小可承载类型
	 *
	 * @param strVal 要判断的值
	 * @return 最小的可承载类型
	 */
	private static BasicType lowestType(String strVal) {
		int dot = 0;
		boolean alpha = false;
		for (int i = 0; i < strVal.length(); i++) {
			char c = strVal.charAt(i);
			if (c == '.') {
				dot++;
			} else if (c > '9' || c < '0') {
				alpha = true;
				break;
			}
		}
		if (alpha == true || dot >= 2) {
			return BasicType.String;
		} else if (dot == 1) {
			return BasicType.Float;
		} else {
			return BasicType.Integer;
		}
	}

	/**
	 * 根据表达式的描述求值
	 *
	 * @param evalDiscriptions 要计算的表达式描述
	 * @param p                计算时需要依赖的数据的指针
	 */
	private UnionOperand eval(List<EvalDiscription> evalDiscriptions, Cursor p)
			throws IOException, ClassNotFoundException {
		Expression exp = new Expression();
		List<String> info = db.getTable(targetTable, tran).getColumnsName();

		for (int i = 0; i < evalDiscriptions.size(); i++) {
			if (evalDiscriptions.get(i).cmd == OpCode.Operand) {
				if (evalDiscriptions.get(i).col_name != null) {

					for (int j = 0; j < info.size(); j++) {
						if (info.get(j).equals(evalDiscriptions.get(i).col_name)) {
							exp.addOperand(new UnionOperand(p.getColumnType(info.get(j)), p.getData().get(j)));
						}
					}

				} else {
					String val = evalDiscriptions.get(i).constant;
					BasicType cType = lowestType(val);
					exp.addOperand(new UnionOperand(cType, val));
				}
			} else {
				exp.applyOperator(evalDiscriptions.get(i).cmd);
			}
		}
		return exp.getAns();
	}

	/**
	 * eval的重载，在下层不提供视图机制的时候用于处理临时表。
	 */
	private UnionOperand eval(List<EvalDiscription> evalDiscriptions, int Index) {
		Expression exp = new Expression();
		List<String> infoJoin = joinResult.getHeaderString();

		for (int i = 0; i < evalDiscriptions.size(); i++) {
			if (evalDiscriptions.get(i).cmd == OpCode.Operand) {
				if (evalDiscriptions.get(i).col_name != null) {

					for (int j = 0; j < infoJoin.size(); j++) {
						if (infoJoin.get(j).equals(evalDiscriptions.get(i).col_name)) {
							//System.out.println(joinResult.getRes().get(Index).get(j));
							//exp.addOperand(new UnionOperand(joinResult.getHeader().get(j).getColumnTypeBasic(),
							//		joinResult.getRes().get(Index).get(j)));
							exp.addOperand(new UnionOperand(BasicType.String,
									joinResult.getRes().get(Index).get(j)));
						}
					}

				} else {
					String val = evalDiscriptions.get(i).constant;
					BasicType cType = lowestType(val);
					exp.addOperand(new UnionOperand(cType, val));
				}
			} else {
				exp.applyOperator(evalDiscriptions.get(i).cmd);
			}
		}
		return exp.getAns();
	}

	private void join(String tableName) throws IOException, ClassNotFoundException {
		Table table = db.getTable(tableName, tran);
		List<Column> fromTreeHead = new ArrayList<>();
		//此处应该加入colnumType,之后见面商量一下
		
		table.getColumnsName().forEach(n -> fromTreeHead.add(new Column(n)));
		Cursor cursor = db.getTable(tableName, tran).createCursor(tran);

		if (joinResult == null) {
			joinResult = new QueryResult(fromTreeHead);
			while (cursor != null) {
				List<String> fromTreeString = cursor.getData();
				joinResult.addRecord(fromTreeString);
				if (cursor.moveToNext(tran) == false) {
					cursor = null;
				}
			}
			return;
		}

		List<List<String>> resList = joinResult.getRes();
		List<Column> resHead = joinResult.getHeader();
		JoinMatch matchedJoin = checkUnion(resHead, fromTreeHead);
		QueryResult copy = new QueryResult(matchedJoin.getJoinHead());

		for (int i = 0; i < resList.size(); i++) {
			List<String> tempRes = resList.get(i);
			while (cursor != null) {
				List<String> fromTreeString = cursor.getData();
				List<String> copyTreeString = new ArrayList<>();
				fromTreeString.forEach(n -> copyTreeString.add(n));

				Iterator iterator = matchedJoin.getJoinUnder().keySet().iterator();
				while (iterator.hasNext()) {
					int nextKey = (Integer) iterator.next();
					int nextValue = matchedJoin.getJoinUnder().get(nextKey);
					String s1 = tempRes.get(nextKey);
					String s2 = fromTreeString.get(nextValue);
					if (!s1.equals(s2)) {
						break;
					} else {
						copyTreeString.remove(nextValue);
					}
				}

				if (iterator.hasNext()) {
					List<String> line = new ArrayList<>();
					tempRes.forEach(n -> line.add(n));
					copyTreeString.forEach(n -> line.add(n));
					copy.getRes().add(line);
				}

				cursor.moveToNext(tran);
			}
		}
		joinResult = copy;
	}

	public JoinMatch checkUnion(List<Column> head1, List<Column> head2) {
		List<Column> unionHead = new ArrayList<>();
		Map<Integer, Integer> unionUnder = new HashMap<>();

		head1.forEach(n -> unionHead.add(n));

		for (Column n : head2) {
			if (!head1.contains(n)) {
				unionHead.add(n);
			}
		}

		for (int i = 0; i < head1.size(); i++) {
			int locate = head2.indexOf(head1.get(i));
			if (locate != -1) {
				unionUnder.put(i, locate);
			}
		}

		return new JoinMatch(unionHead, unionUnder);
	}

	// 这个方法只用于测试自然连接操作。
	public QueryResult forTestJoin(JoinMatch joinMatch, QueryResult input1, QueryResult input2) {
		int matchCount = 0;
		QueryResult copy = new QueryResult(joinMatch.getJoinHead());
		List<List<String>> resList = input1.getRes();
		for (int i = 0; i < resList.size(); i++) {
			List<String> tempRes = resList.get(i);
			for (List<String> fromTreeString : input2.getRes()) {
				List<String> copyTreeString = new ArrayList<>();
				fromTreeString.forEach(n -> copyTreeString.add(n));
				Iterator iterator = joinMatch.getJoinUnder().keySet().iterator();
				matchCount = 0;

				while (iterator.hasNext()) {
					int nextKey = (Integer) iterator.next();
					int nextValue = joinMatch.getJoinUnder().get(nextKey);
					String s1 = tempRes.get(nextKey);
					String s2 = fromTreeString.get(nextValue);
					if (!s1.equals(s2)) {
						break;
					} else {
						matchCount++;
						copyTreeString.remove(nextValue);
					}
				}

				if (matchCount == joinMatch.getJoinUnder().size()) {
					List<String> line = new ArrayList<>();
					tempRes.forEach(n -> line.add(n));
					copyTreeString.forEach(n -> line.add(n));
					copy.getRes().add(line);
				}
			}
		}
		return copy;
	}

}