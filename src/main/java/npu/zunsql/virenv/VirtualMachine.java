package npu.zunsql.virenv;

import npu.zunsql.treemng.*;
//import sun.security.provider.JavaKeyStore.CaseExactJKS;
//import sun.security.provider.JavaKeyStore.CaseExactJKS;

import java.io.IOException;
import java.util.*;

//import javax.sound.sampled.Port.Info;

public class VirtualMachine {
	// 浣滀负杩囨护鍣ㄦ潵瀵硅褰曡繘琛岀瓫閫�
	private List<EvalDiscription> filters;
	// 瀛樺偍琚�夊嚭鐨勫垪
	private List<String> selectedColumns;
	// 瀛樺偍瑕佹彃鍏ョ殑璁板綍
	private List<AttrInstance> record;
	// 瀛樺偍瑕佸垱寤鸿〃鐨勫悇椤硅〃澶达紝璇ユ暟鎹粨鏋勪粎鐢ㄤ簬鍒涘缓琛�
	private List<Column> columns;
	// 瀛樺偍execute鎸囦护鎵ц鍚庣殑鏌ヨ缁撴瀯锛屼粎select鎸囦护瀵瑰簲鐨勬搷浣滀細浣垮緱璇ラ泦鍚堥潪绌�
	private QueryResult result;
	// 瑕佹搷浣滅殑瀵硅薄琛ㄥ悕
	private String targetTable;
	// 鍒涘缓琛ㄦ椂涓婚敭鐨勫悕绉板瓨鍌ㄥ湪璇ュ彉閲忎腑
	private String pkName;
	// 瑕佹洿鏂扮殑灞炴�у悕绉帮紝椤哄簭蹇呴』涓庝笅涓�涓彉閲忕殑椤哄簭涓�鑷�
	private List<String> updateAttrs;
	// 瑕佹洿鏂扮殑灞炴�у�硷紝椤哄簭蹇呴』涓庝笂涓�涓彉閲忕殑椤哄簭涓�鑷�
	private List<List<EvalDiscription>> updateValues;
	// 涓存椂鍙橀噺
	private List<EvalDiscription> singleUpdateValue;
	// 璁板綍鏈execute灏嗘墽琛岀殑鍛戒护
	private Activity activity;
	// 浣滀负join鎿嶄綔鐨勭粨鏋滈泦
	private QueryResult joinResult;
	// 浜嬪姟鍙ユ焺
	private Transaction tran;
	private Transaction usertran;

	private boolean isJoin = false;
	private int joinIndex = 0;

	private boolean suvReadOnly;
	private boolean recordReadOnly;
	private boolean columnsReadOnly;
	private boolean selectedColumnsReadOnly;
	private Database db;

	private boolean isUserTransaction = false;

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

		usertran = null;
		isUserTransaction = false;
	}

	public QueryResult run(List<Instruction> instructions) throws Exception {

		for (Instruction cmd : instructions) {
			 System.out.println(cmd.opCode + " " + cmd.p1 + " " + cmd.p2 + " " + cmd.p3);
			run(cmd);
			//try catch 出现错误上面的指令全部rollback
		}
		System.out.println("\n");
		// isJoin= false;
		return result;
	}

	private void run(Instruction instruction) throws IOException, ClassNotFoundException {
		OpCode opCode = instruction.opCode;
		String p1 = instruction.p1;
		String p2 = instruction.p2;
		String p3 = instruction.p3;

		// 鎵�鏈夋搷浣滈兘鏄欢鏃舵搷浣滐紝鍗冲湪execute鍚庣敓鏁堬紝鍏朵粬鍛戒护鍙細鍚慥M涓～鍏呬俊鎭�
		// 鐗逛緥鏄痗ommit鎸囦护鍜宺ollback鎸囦护浼氱珛鍗虫墽琛�
		switch (opCode) {
		// 涓嬮潰鏄叧浜庝簨鍔＄殑澶勭悊浠ｇ爜
		case Transaction:
			ConditonClear();
			// 濡傛灉杩欓噷涓嶈兘鎻愪緵Transaction鐨勭被鍨嬶紝閭ｄ箞鍙兘鍦╡xecute鐨勬椂鍊欑敱铏氭嫙鏈烘潵鑷姩鎺ㄦ柇
			// 杩欓噷涓嶅仛浠讳綍澶勭悊锛屽洜涓轰笂涓�灞傚苟娌℃湁浜ょ粰鏈眰浜嬪姟绫诲瀷
			break;

		case Begin:
//			usertran = db.beginUserTrans();
			tran = db.beginWriteTrans();
			isUserTransaction = true;
			break;

		case UserCommit:
			try {
				tran.Commit();
			} catch (IOException e) {
				Util.log("鎻愪氦澶辫触");
				throw e;
			}
			tran = null;
			isUserTransaction = false;
			break;

		case Commit:
			try {
				if (!isUserTransaction) {
					tran.Commit();
				}
				ConditonClear();
			} catch (IOException e) {
				Util.log("鎻愪氦澶辫触");
				throw e;
			}
			break;

		case Rollback:
//			usertran.RollBack();
//			usertran = null;
			tran.RollBack();
			isUserTransaction = false;
			try {
				db.close();
				db = new Database(db.getDatabaseName());
			} catch (IOException ie) {
				ie.printStackTrace();
				System.exit(-1);
			} catch (ClassNotFoundException ce) {
				ce.printStackTrace();
				System.exit(-1);
			}
			break;

		// 涓嬮潰鏄垱寤鸿〃鐨勫鐞嗕唬鐮�
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
			// 鍦ㄥ彧鏀寔涓�涓睘鎬т綔涓轰富閿殑鏉′欢涓嬶紝姝ゆ搷浣滄湰鏃犳剰涔�
			// 浣嗘寚瀹氫富閿剰鍛崇潃灞炴�т俊鎭緭鍏ュ畬姣曪紝鍥犳灏哻olumnsReadOnly缃负true
			columnsReadOnly = true;
			break;

		case AddPK:
			// 鍦ㄥ彧鏀寔涓�涓睘鎬т綔涓轰富閿殑鏉′欢涓嬶紝鐩存帴瀵筽kName璧嬪�煎嵆鍙�
			pkName = p1;
			break;

		case EndPK:
			// 鍦ㄥ彧鏀寔涓�涓睘鎬т綔涓轰富閿殑鏉′欢涓嬶紝姝ゆ搷浣滄棤鎰忎箟
			// 鏆傛椂灏嗘鍛戒护浣滀负createTable缁撴潫鐨勬爣蹇�
			break;

		// 涓嬮潰鏄垹闄よ〃鐨勬搷浣�
		case DropTable:
			activity = Activity.DropTable;
			targetTable = p3;
			break;

		// 涓嬮潰鏄彃鍏ユ搷浣滐紝杩欐槸涓欢鏃舵搷浣�
		case Insert:
			activity = Activity.Insert;
			targetTable = p3;
			record.clear();
			updateValues.clear();

			break;

		// 涓嬮潰鏄垹闄ゆ搷浣滐紝杩欐槸涓欢鏃舵搷浣�
		case Delete:
			activity = Activity.Delete;
			targetTable = p3;
			break;

		// 涓嬮潰鏄�夋嫨鎿嶄綔锛岃繖鏄釜寤舵椂鎿嶄綔
		case Select:
			activity = Activity.Select;
			// targetTable = p3;

			break;

		// 涓嬮潰鏄洿鏂版搷浣滐紝杩欐槸涓欢鏃舵搷浣�
		case Update:
			activity = Activity.Update;
			targetTable = p3;
			break;

		// 涓嬮潰鏄叧浜庢彃鍏ヤ竴鏉¤褰曠殑鍐呭鐨勬搷浣�
		case BeginItem:
			recordReadOnly = false;
			break;

		case AddItemCol:
			record.add(new AttrInstance(p1, p2, p3));

		case EndItem:
			recordReadOnly = true;
			break;

		// 鍏充簬閫夋嫨鍣ㄧ殑閫夐」锛岃繖閲屽�熷姪琛ㄨ揪寮忓疄鐜帮紝浠呭湪鏈�鍚庡皢璁板綍鐨勮〃杈惧紡浼犵粰filters
		case BeginFilter:
			suvReadOnly = false;
			singleUpdateValue = new ArrayList<>();
			break;

		case EndFilter:
			filters = singleUpdateValue;
			// System.out.println("filters name"+filters.get(0).col_name);
			suvReadOnly = true;
			break;

		// 涓嬮潰鏄叧浜巗elect閫夋嫨鐨勫睘鎬х殑璁剧疆
		case BeginColSelect:
			selectedColumnsReadOnly = false;
			break;

		case AddColSelect:
			selectedColumns.add(p1);
			break;

		case EndColSelect:
			selectedColumnsReadOnly = true;
			break;

		// 涓嬮潰鏄鐞嗛�夋嫨鐨勮〃鐨勮繛鎺ユ搷浣滅殑浠ｇ爜
		case BeginJoin:
			// 鎺ユ敹鍒癹oin鍛戒护锛屾竻绌轰复鏃惰〃
			joinResult = null;
			isJoin = true;
			joinIndex = 0;
			if (!isUserTransaction) {
				tran = db.beginReadTrans();
			}
			break;

		case AddTable:
			targetTable = p1;
			// 璋冪敤涓嬪眰鏂规硶锛屽姞杞絧1琛紝灏嗚嚜鐒惰繛鎺ョ殑缁撴灉瀛樺叆joinResult
			join(targetTable);
			break;

		case EndJoin:
			break;

		// 涓嬮潰鐨勪唬鐮佽缃畊pdate瑕佹洿鏂扮殑鍊硷紝褰㈠紡涓篶olName=Expression
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

		// 璁板綍Expression鎻忚堪鐨勪唬鐮�
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
			Util.log("娌℃湁杩欐牱鐨勫瓧鑺傜爜: " + opCode + " " + p1 + " " + p2 + " " + p3);
			break;

		}
	}

	private void ConditonClear() throws IOException, ClassNotFoundException {
		recordReadOnly = true;
		columnsReadOnly = true;
		selectedColumnsReadOnly = true;
		suvReadOnly = true;
		filters.clear();

		// tran = null;
		// result = null;
		selectedColumns.clear();
		record.clear();
		columns.clear();
		updateAttrs.clear();
		updateValues.clear();
		singleUpdateValue.clear();
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
		if (!isUserTransaction) {
			tran = db.beginWriteTrans();
		}
		if (db.dropTable(targetTable, tran) == false) {
			Util.log("鍒犻櫎琛ㄥけ璐�");
		}
	}

	private void createTable() throws IOException, ClassNotFoundException {
		// 闇�瑕佸紑鍚竴涓啓浜嬪姟
		if (!isUserTransaction) {
			tran = db.beginWriteTrans();
		}

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
			Util.log("鍒涘缓琛ㄥけ璐�");
		}
	}

	/**
	 * 妫�鏌ュ綋鍓嶈褰曟槸鍚︽弧瓒硍here瀛愬彞鐨勬潯浠�
	 *
	 * @param p 褰撳墠琛ㄤ笂鐨勬寚閽�
	 * @return 婊¤冻鏉′欢杩斿洖true锛屽惁鍒欒繑鍥瀎alse
	 */
	private boolean check(Cursor p) throws IOException, ClassNotFoundException {
		// 濡傛灉娌℃湁where瀛愬彞锛岄偅涔堣繑鍥瀟rue锛屽嵆瀵规墍鏈夎褰曢兘鎵ц鎿嶄綔
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
			Util.log("where瀛愬彞鐨勮〃杈惧紡杩斿洖鍊间笉鑳戒负String");
			return false;
		} else if (Math.abs(Double.valueOf(ans.getValue())) < 1e-10) {
			return false;
		} else {
			return true;
		}
	}

	// 鏍规嵁joinIndex妫�娴嬭鏉¤褰曟槸鍚︽弧瓒砯ilter
	private boolean check(int Index) throws IOException, ClassNotFoundException {
		// 濡傛灉娌℃湁where瀛愬彞锛岄偅涔堣繑鍥瀟rue锛屽嵆瀵规墍鏈夎褰曢兘鎵ц鎿嶄綔
		if (filters.size() == 0) {
			return true;
		}

		UnionOperand ans;
		ans = eval(filters, Index);
		if (ans.getType() == BasicType.String) {
			Util.log("where瀛愬彞鐨勮〃杈惧紡杩斿洖鍊间笉鑳戒负String");
			return false;
		} else if (Math.abs(Double.valueOf(ans.getValue())) < 1e-10) {
			return false;
		} else {
			return true;
		}
	}

	private void select() throws IOException, ClassNotFoundException {

		// 鏋勯�犵粨鏋滈泦鐨勮〃澶�
		List<Column> selected = new ArrayList<>();
		List<String> temp;
		for (String colName : selectedColumns) {
			Column col = new Column(colName);
			selected.add(col);
		}
		result = new QueryResult(selected);

		if (isJoin) {

			if (selected.get(0).getColumnName().equals("*")) {
				for (int indexi = 0; indexi < joinResult.getRes().size(); ++indexi) {
					if (check(indexi)) {
						result.addRecord(joinResult.getRes().get(indexi));
						result.addAffectedCount();
					}

				}
				return;

			}
			temp = joinResult.getHeaderString();

			// 鐢ㄤ簬joinResult鐨勫惊鐜尮閰嶃��
			for (int k = 0; k < joinResult.getRes().size(); k++, joinIndex++) {
				// 姝ゅ搴旇妫�娴媕oinResult.get(k)鏄惁婊¤冻filter
				if (check(k)) {
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
		if (!isUserTransaction) {
			tran = db.beginWriteTrans();
		}

//        //鍥犱笅灞傛湭鎻愪緵鎺ュ彛锛屾殏鏃舵敞閲婃帀
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
	 * 瀵瑰叏琛ㄨ繘琛屾洿鏂�
	 */
	private void update() throws IOException, ClassNotFoundException {
		if (!isUserTransaction) {
			tran = db.beginWriteTrans();
		}
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
					// 鏌ヨ瑕佹洿鏂扮殑灞炴�х殑淇℃伅骞跺垱寤篶ell瀵硅薄鏉ユ墽琛屾洿鏂�
					// String attrname = record.get(i).attrName;
					String attrname = updateAttrs.get(i);
					// String attrname = "name";
					// System.out.println("record
					// name:"+attrname+",updateAttrs.size="+updateAttrs.size());
					// 寰幆鐨勬柟寮忔槸鍚︽纭�?
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
	 * 灏嗕竴鏉¤褰曟彃鍏ュ埌琛ㄤ腑 鍥犱负涓婂眰娌℃湁浜х敓default锛屼笅灞備篃鏈彁渚涙帴鍙ｏ紝鍥犳杩欓噷姣忔鍙兘鎻掑叆涓�鏉″畬鏁寸殑璁板綍
	 */
	private void insert() throws IOException, ClassNotFoundException {
		if (!isUserTransaction) {
			tran = db.beginWriteTrans();
		}
		List<String> colValues = new ArrayList<>();

		for (List<EvalDiscription> item : updateValues) {
			colValues.add(eval(item, null).getValue());
		}

		if (db.getTable(targetTable, tran).createCursor(tran).insert(tran, colValues)) {
			result.addAffectedCount();
		}
	}

	/**
	 * 纭畾涓�涓瓧绗︿覆鍊肩殑鏈�灏忓彲鎵胯浇绫诲瀷
	 *
	 * @param strVal 瑕佸垽鏂殑鍊�
	 * @return 鏈�灏忕殑鍙壙杞界被鍨�
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
	 * 鏍规嵁琛ㄨ揪寮忕殑鎻忚堪姹傚��
	 *
	 * @param evalDiscriptions 瑕佽绠楃殑琛ㄨ揪寮忔弿杩�
	 * @param p                璁＄畻鏃堕渶瑕佷緷璧栫殑鏁版嵁鐨勬寚閽�
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
	 * eval鐨勯噸杞斤紝鍦ㄤ笅灞備笉鎻愪緵瑙嗗浘鏈哄埗鐨勬椂鍊欑敤浜庡鐞嗕复鏃惰〃銆�
	 */
	private UnionOperand eval(List<EvalDiscription> evalDiscriptions, int Index) {
		Expression exp = new Expression();
		List<String> infoJoin = joinResult.getHeaderString();

		for (int i = 0; i < evalDiscriptions.size(); i++) {
			if (evalDiscriptions.get(i).cmd == OpCode.Operand) {
				if (evalDiscriptions.get(i).col_name != null) {

					for (int j = 0; j < infoJoin.size(); j++) {
						if (infoJoin.get(j).equals(evalDiscriptions.get(i).col_name)) {
							// System.out.println(joinResult.getRes().get(Index).get(j));
							exp.addOperand(new UnionOperand(joinResult.getHeader().get(j).getColumnTypeBasic(),
									joinResult.getRes().get(Index).get(j)));
							// exp.addOperand(new UnionOperand(BasicType.String,
							// joinResult.getRes().get(Index).get(j)));
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
		// 姝ゅ搴旇鍔犲叆colnumType,涔嬪悗瑙侀潰鍟嗛噺涓�涓�

		table.getColumnsName().forEach(n -> fromTreeHead.add(new Column(n)));
		List<BasicType> types = table.getColumnsType();

		for (int i = 0; i < types.size(); ++i) {
			fromTreeHead.get(i).ColumnType = types.get(i).toString();
		}

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

		// List<List<String>> resList = joinResult.getRes();
		// List<Column> resHead = joinResult.getHeader();
		// JoinMatch matchedJoin = checkUnion(resHead, fromTreeHead);
		// QueryResult copy = new QueryResult(matchedJoin.getJoinHead());
		// 寰楀埌join缁撴灉鐨勫ご锛屽嵆鍒楄〃鍚�
		List<Column> joinHead = joinResult.getHeader();
		int snglJoin = joinResult.getHeader().size();
		table.getColumnsName().forEach(n -> joinHead.add(new Column(n)));
		for (int ndx1 = snglJoin; ndx1 < snglJoin + types.size(); ++ndx1) {
			joinHead.get(ndx1).ColumnType = types.get(ndx1 - snglJoin).toString();
		}

		// 灏嗕袱涓〃杩涜鍏ㄨ繛鎺ワ紝浣滀负涓�涓〃杩涜鍒ゆ柇

		QueryResult joinRes = new QueryResult(joinHead);

		for (int ndx1 = 0; ndx1 < joinResult.getRes().size(); ++ndx1) {
			// for(int ndx2=0; ndx2<table.getColumnsName().size(); ++ndx2){
			while (cursor != null) {
				List<String> snglRecord = new ArrayList<>();
//				System.out.println(joinResult.getRes().get(ndx1));
				for (int arri = 0; arri < joinResult.getRes().get(ndx1).size(); ++arri) {
					snglRecord.add(joinResult.getRes().get(ndx1).get(arri));
				}
				// joinResult.getRes().get(ndx1).forEach(n -> snglRecord.add(n));
				for (int ndx3 = 0; ndx3 < cursor.getData().size(); ++ndx3) {
					snglRecord.add(cursor.getData().get(ndx3));
				}
				joinRes.addRecord(snglRecord);
				if (cursor.moveToNext(tran) == false) {
					cursor = null;
				}
			}
			cursor = db.getTable(tableName, tran).createCursor(tran);
			// }
		}
		joinResult = joinRes;
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

	// 杩欎釜鏂规硶鍙敤浜庢祴璇曡嚜鐒惰繛鎺ユ搷浣溿��
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