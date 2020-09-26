package npu.zunsql.virenv;

import npu.zunsql.treemng.*;
//import sun.security.provider.JavaKeyStore.CaseExactJKS;
//import sun.security.provider.JavaKeyStore.CaseExactJKS;

import java.io.IOException;
import java.util.*;

//import javax.sound.sampled.Port.Info;

public class VirtualMachine {
    //作为过滤器来对记录进行筛选
	private List<EvalDiscription> filters;
    //存储被选出的列
	private List<String> selectedColumns;
    //存储要插入的记录
	private List<AttrInstance> record;
    //存储要创建表的各项表头，该数据结构仅用于创建表
	private List<Column> columns;
	//存储order by多列
	private List<String> ordercol;
	//记录order by是升序还是降序
	private int orderflag = 0;
	//记录groupby存储的列
	private List<String> groupcol; 
	//记录要进行sum、的列
	private String sumcol;
    //存储execute指令执行后的查询结构，仅select指令对应的操作会使得该集合非空
	private QueryResult result;
    //要操作的对象表名
	private String targetTable;
    //创建表时主键的名称存储在该变量中
	private String pkName;
    //要更新的属性名称，顺序必须与下一个变量的顺序一致
	private List<String> updateAttrs;
    //要更新的属性值，顺序必须与上一个变量的顺序一致
	private List<List<EvalDiscription>> updateValues;
    //临时变量
	private List<EvalDiscription> singleUpdateValue;
    //记录本次execute将执行的命令
	private Activity activity;
    //作为join操作的结果集
	private QueryResult joinResult;
    //事务句柄
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
		sumcol = null;

		filters = new ArrayList<>();
		selectedColumns = new ArrayList<>();
		ordercol = new ArrayList<>();
		record = new ArrayList<>();
		columns = new ArrayList<>();
		updateAttrs = new ArrayList<>();
		updateValues = new ArrayList<>();
		singleUpdateValue = new ArrayList<>();
		groupcol = new ArrayList<>();

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

        //所有操作都是延时操作，即在execute后生效，其他命令只会向VM中填充信息
        //特例是commit指令和rollback指令会立即执行
		switch (opCode) {
            //下面是关于事务的处理代码
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
				Util.log("提交失败");
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
				Util.log("提交失败");
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

            //下面是创建表的处理代码
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
                //在只支持一个属性作为主键的条件下，此操作本无意义
                //但指定主键意味着属性信息输入完毕，因此将columnsReadOnly置为true
                columnsReadOnly = true;
                break;

            case AddPK:
                //在只支持一个属性作为主键的条件下，直接对pkName赋值即可
                pkName = p1;
                break;

            case EndPK:
                //在只支持一个属性作为主键的条件下，此操作无意义
                //暂时将此命令作为createTable结束的标志
                break;

            //下面是删除表的操作
            case DropTable:
                activity = Activity.DropTable;
                targetTable = p3;
                break;

            //下面是插入操作，这是个延时操作
            case Insert:
                activity = Activity.Insert;
                targetTable = p3;
                record.clear();
                updateValues.clear();
                break;

            //下面是删除操作，这是个延时操作
            case Delete:
                activity = Activity.Delete;
                targetTable = p3;
                break;

            //下面是选择操作，这是个延时操作
		case Select:
			activity = Activity.Select;
			// targetTable = p3;

			break;

            //下面是更新操作，这是个延时操作
            case Update:
                activity = Activity.Update;
                targetTable = p3;
                break;

            //下面是关于插入一条记录的内容的操作
            case BeginItem:
                recordReadOnly = false;
                break;

		case AddItemCol:
			record.add(new AttrInstance(p1, p2, p3));

		case EndItem:
			recordReadOnly = true;
			break;

            //关于选择器的选项，这里借助表达式实现，仅在最后将记录的表达式传给filters
            case BeginFilter:
                suvReadOnly = false;
                singleUpdateValue = new ArrayList<>();
                break;

		case EndFilter:
			filters = singleUpdateValue;
			// System.out.println("filters name"+filters.get(0).col_name);
			suvReadOnly = true;
			break;

            //下面是关于select选择的属性的设置
            case BeginColSelect:
                selectedColumnsReadOnly = false;
                break;

		case AddColSelect:
			selectedColumns.add(p1);
			break;

		case EndColSelect:
			selectedColumnsReadOnly = true;
			break;

            //下面是处理选择的表的连接操作的代码
		case BeginJoin:
                //接收到join命令，清空临时表
			joinResult = null;
			isJoin = true;
			joinIndex = 0;
			if (!isUserTransaction) {
				tran = db.beginReadTrans();
			}
			break;

		case AddTable:
			targetTable = p1;
                //调用下层方法，加载p1表，将自然连接的结果存入joinResult
			join(targetTable);
			break;

		case EndJoin:
			break;

            //下面的代码设置update要更新的值，形式为colName=Expression
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

            //记录Expression描述的代码
		case Operand:
			singleUpdateValue.add(new EvalDiscription(opCode, p1, p2));
			// System.out.println("###singleUpdateValue:"+singleUpdateValue.get(0).cmd+" "+
			// singleUpdateValue.get(0).col_name+" "+singleUpdateValue.get(0).constant);
			break;

		case Operator:
			singleUpdateValue.add(new EvalDiscription(OpCode.valueOf(p1), null, null));
			break;

		case Order:
			ordercol.add(p1);
			if(p2 == "desc")
			{
				orderflag = 2;
			}
			else
			{
				orderflag = 1;
			}
			break;

		case Group:
			groupcol.add(p1);
			break;
		
		case Sum:
			sumcol = p1;
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
		filters.clear();

		// tran = null;
		// result = null;
		selectedColumns.clear();
		record.clear();
		columns.clear();
		updateAttrs.clear();
		updateValues.clear();
		singleUpdateValue.clear();
		ordercol.clear();
		orderflag = 0;
		activity = null;
		targetTable = null;
		joinResult = null;
		groupcol.clear();
		sumcol = null;
	}

	private void execute() throws IOException, ClassNotFoundException {
		result = new QueryResult();
		switch (activity) {
		case Select:
			//sumcol = "score";
			//groupcol.add("course");
			if(orderflag != 0)
			{
				orderby_sort(ordercol , orderflag , joinResult);
			}
			if(!groupcol.isEmpty())
			{
				orderby_sort(groupcol , 1 , joinResult);
			}
			select();
			// ConditonClear();
			//orderflag = 2;
			//ordercol.add("score");
			aggregate();
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
            Util.log("删除表失败");
		}
	}

	private void createTable() throws IOException, ClassNotFoundException {
        //需要开启一个写事务
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
        //如果没有where子句，那么返回true，即对所有记录都执行操作
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

	// 鏍规嵁joinIndex妫�娴嬭鏉¤褰曟槸鍚︽弧瓒砯ilter
	private boolean check(int Index) throws IOException, ClassNotFoundException {
		// 濡傛灉娌℃湁where瀛愬彞锛岄偅涔堣繑鍥瀟rue锛屽嵆瀵规墍鏈夎褰曢兘鎵ц鎿嶄綔
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

        //构造结果集的表头
        List<Column> selected = new ArrayList<>();
        List<String> temp;
        for (String colName : selectedColumns) {
            Column col = new Column(colName);
            selected.add(col);
        }
        
        //若使用聚集函数，需要加上新列
        if (sumcol != null)
        {
        	Column col = new Column("SUM(" + sumcol + ")");
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
				
				// 重新加入表头，以显示实际的列名，而不是"*"
				result.addHeader((ArrayList<Column>) joinResult.getHeader()); // 从连接结果中获取表头，并且将其加入到最终的结果中，以显示
				return;

			}
			temp = joinResult.getHeaderString();

                //用于joinResult的循环匹配。
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
		if (!isUserTransaction) {
			tran = db.beginWriteTrans();
		}
		Cursor p = db.getTable(targetTable, tran).createCursor(tran);
		List<String> header = db.getTable(targetTable, tran).getColumnsName();
		while (p != null) {
			List<String> row = p.getData();
			if (check(p)) {

				for (int i = 0; i < updateAttrs.size(); i++) {
					// 查询要更新的属性的信息并创建cell对象来执行更新
					String attrname = updateAttrs.get(i);
					// 寰幆鐨勬柟寮忔槸鍚︽纭�?
					for (int j = 0; j < header.size(); j++) {

						if (header.get(j).equals(attrname)) {
							row.set(j, eval(updateValues.get(i), p).getValue());
						}
					}
				}
			}
			if (p.setData(tran, row)) {
				result.addAffectedCount();
			}
			if (false == p.moveToNext(tran)) {
				p = null;
			}
		}
	}

	/**
	 * 将一条记录插入到表中
	 * 因为上层没有产生default，下层也未提供接口，因此这里每次只能插入一条完整的记录
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
							if (!joinResult.getRes().get(Index).isEmpty())
							{
								exp.addOperand(new UnionOperand(joinResult.getHeader().get(j).getColumnTypeBasic(),
										joinResult.getRes().get(Index).get(j)));
							}
							else
							{
								// 如果“自然连接表”为空，这里填写null，在后面的applyOperator方法中集中处理
								exp.addOperand(null);
							}
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
			while (cursor != null) {
				List<String> snglRecord = new ArrayList<>();
				for (int arri = 0; arri < joinResult.getRes().get(ndx1).size(); ++arri) {
					snglRecord.add(joinResult.getRes().get(ndx1).get(arri));
				}
				for (int ndx3 = 0; ndx3 < cursor.getData().size(); ++ndx3) {
					snglRecord.add(cursor.getData().get(ndx3));
				}
				joinRes.addRecord(snglRecord);
				if (cursor.moveToNext(tran) == false) {
					cursor = null;
				}
			}
			cursor = db.getTable(tableName, tran).createCursor(tran);
		}
		joinResult = joinRes;
	}
	
	private void orderby_sort(List<String> ordercol1 , Integer orderflag1 , QueryResult list) throws IOException, ClassNotFoundException{
		List<Integer> temp_col = new ArrayList<>();

		for(int i = 0 ; i < ordercol1.size() ; i ++)
		{
			String temp = ordercol1.get(i);
			for(int j = 0 ; j < list.getHeader().size() ; j ++)
			{
				if(list.getHeader().get(j).getColumnName().compareTo(temp) == 0)
				{
					temp_col.add(j);
					break;
				}
			}
		}
		
		Collections.sort(list.getRes() , new Comparator<List<String>>() {
            @Override
            public int compare(List<String> p1, List<String> p2) {
            	for(int i = 0 ; i < temp_col.size() ; i ++)
            	{
            		int j = temp_col.get(i);
            		if(p1.get(j).compareTo(p2.get(j)) > 0)
            		{
            			if(orderflag1 == 1)
            			{
            				return 1;
            			}
            			else
            			{
            				return -1;
            			}
            		}
            		else if(p1.get(j).compareTo(p2.get(j)) < 0)
            		{
            			if(orderflag1 == 1)
            			{
            				return -1;
            			}
            			else
            			{
            				return 1;
            			}
            		}
            	}
                return 1;
            }
            
        });
	}
	
	private void aggregate() throws IOException, ClassNotFoundException{
		if(sumcol != null)
		{
			if(groupcol.isEmpty())
			{
				double sum = 0 ;
				int temp = -1;
				//找到要进行sum操作的列
				for(int i = 0 ; i < joinResult.getHeader().size() ; i ++)
				{
					if(joinResult.getHeader().get(i).getColumnName().equals(sumcol))
					{
						temp = i;
						break;
					}
				}
				//进行sum操作
				for(int i = 0 ; i < joinResult.getRes().size() ; i ++)
				{
					if(check(i))
					{
						sum += Double.parseDouble(joinResult.getRes().get(i).get(temp));
					}
				}
				//删除result中原有元素
				result.getRes().clear();
				//加入sum结果行
				List<String> ansRecord = new ArrayList<>();
				ansRecord.add(String.valueOf(sum));
				result.addRecord(ansRecord);
			}
			else
			{
				int temp1 = -1 , count = 0;
				List<Integer> temp2 = new ArrayList<>();
				//找到要进行sum操作的列
				for(int i = 0 ; i < joinResult.getHeader().size() ; i ++)
				{
					if(joinResult.getHeader().get(i).getColumnName().equals(sumcol))
					{
						temp1 = i;
						break;
					}
				}
				//找到要groupby的列
				for(int i = 0 ; i < groupcol.size(); i ++)
				{
					for(int j = 0 ; j < joinResult.getHeader().size() ; j ++)
					{
						if(joinResult.getHeader().get(j).getColumnName().equals(groupcol.get(i)))
						{
							temp2.add(j);
							break;
						}
					}
				}
				//对joinResult以及result进行一次排序
				//orderby_sort(groupcol , 1 , joinResult);
				//orderby_sort(groupcol , 1 , result);
				//计算sum
				int tempflag = 1;
				List<String> temprow = new ArrayList<>();
				List<Double> sum = new ArrayList<>();
				for(int i = 0 ; i < joinResult.getRes().size() ; i ++)
				{
					if(check(i))
					{
						String tempstring = "";
						for(int j = 0 ; j < groupcol.size(); j ++)
						{
							tempstring += joinResult.getRes().get(i).get(temp2.get(j));
						}

						if(temprow.contains(tempstring))
						{
							sum.set(sum.size() - 1 , sum.get(sum.size() - 1) + Double.parseDouble(joinResult.getRes().get(i).get(temp1)));		
						}
						else
						{
							temprow.add(tempstring);
							sum.add(Double.parseDouble(joinResult.getRes().get(i).get(temp1)));
						}
					}
				}
				//找到group所在列
				List<Integer> flag = new ArrayList<>();
				for(int i = 0 ; i < result.getHeader().size() ; i ++)
				{
					if(result.getHeader().get(i).getColumnName().equals(groupcol))
					{
						flag.add(i);
					}
				}
				//回填sum，删除多余行
				temprow.clear();
				int sum_count = 0 , iter_count = 0;
				Iterator<List<String>> it = result.getRes().iterator();
				while(it.hasNext())
				{
					List<String> templist = new ArrayList<>();
					templist = it.next();
					String tempstring = "";
					for(int j = 0 ; j < groupcol.size(); j ++)
					{
						tempstring += joinResult.getRes().get(iter_count).get(temp2.get(j));
					}
					
					if(temprow.contains(tempstring))
					{
						it.remove();		
					}
					else
					{
						temprow.add(tempstring);
						//result.getRes().get(iter_count).add(sum.get(sum_count));
						templist.add(String.valueOf(sum.get(sum_count)));
						sum_count ++;
					}
					iter_count ++;
				}
				
			}
		}
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