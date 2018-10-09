package npu.zunsql.treemng;

import npu.zunsql.cache.CacheMgr;
import npu.zunsql.cache.Page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class Table implements TableReader, Serializable {
	protected String tableName;

	protected Column keyColumn;

	protected List<Column> columns;

	protected LockType lock;

	protected int rootNodePage;

	// page层的Mgr，用于对Page层进行操作。
	public CacheMgr cacheManager;

	private Page pageOne;

	private boolean writeMyPage(Transaction myTran) {
		// 写本页
		return cacheManager.writePage(myTran.tranNum, pageOne);
	}

	private void intoBytes(Transaction thisTran) throws IOException {
		byte[] bytes = new byte[Page.PAGE_SIZE];
		ByteArrayOutputStream byt = new ByteArrayOutputStream();

		ObjectOutputStream obj = new ObjectOutputStream(byt);
		obj.writeObject(tableName);
		obj.writeObject(keyColumn);
		obj.writeObject(columns);
		obj.writeObject(lock);
		obj.writeObject(rootNodePage);
		bytes = byt.toByteArray();
		pageOne.getPageBuffer().rewind();
		pageOne.getPageBuffer().put(bytes);
		cacheManager.writePage(thisTran.tranNum, pageOne);
		// thisTran.Commit();
	}

	// 已有page，只需要加载其中的信息。
	// 新建table的工作在database中已经完成，因此，可能加载出只有表头的空表。
	protected Table(int pageID, CacheMgr cacheManager, Transaction thisTran)
			throws IOException, ClassNotFoundException {
		super();
		this.cacheManager = cacheManager;
		pageOne = this.cacheManager.readPage(thisTran.tranNum, pageID);

		ByteBuffer thisBufer = pageOne.getPageBuffer();
		thisBufer.rewind();
		byte[] bytes = new byte[Page.PAGE_SIZE];
		thisBufer.get(bytes, 0, thisBufer.remaining());

		ByteArrayInputStream byteTable = new ByteArrayInputStream(bytes);
		ObjectInputStream objTable = new ObjectInputStream(byteTable);

		this.tableName = (String) objTable.readObject();
		this.keyColumn = (Column) objTable.readObject();
		this.columns = (List<Column>) objTable.readObject();
		this.lock = (LockType) objTable.readObject();
		this.rootNodePage = (int) objTable.readObject();

	}

	protected Integer getTablePageID() {
		return pageOne.getPageID();
	}

	protected Column getKeyColumn() {
		return keyColumn;
	}

	protected Node getRootNode(Transaction thisTran) throws IOException, ClassNotFoundException {
		if (rootNodePage < 0) {
			return null;
		} else {
			return new Node(rootNodePage, cacheManager, thisTran);
		}
	}

	protected Column getColumn(String columnName) {
		for (int i = 0; i < columns.size(); i++) {
			if (columns.get(i).getName().equals(columnName)) {
				return columns.get(i);
			}
		}
		return null;
	}

	protected void writeRootNodePage(int id, Transaction thisTran) throws IOException {
		rootNodePage = id;
		intoBytes(thisTran);

	}

	public Cursor createCursor(Transaction thistran) throws IOException, ClassNotFoundException {
		return new TableCursor(this, thistran); // NULL
	}

	public List<String> getColumnsName() {
		List<String> sList = new ArrayList<String>();
		for (int i = 0; i < columns.size(); i++) {
			sList.add(columns.get(i).getName());
		}
		return sList;
	}

	public List<BasicType> getColumnsType() {
		List<BasicType> sList = new ArrayList<BasicType>();
		for (int i = 0; i < columns.size(); i++) {
			sList.add(columns.get(i).getType());
		}
		return sList;
	}

	public String getTableName() {
		return tableName; // NULL
	}

	public boolean isLocked() {
		if (lock == LockType.Locked) {
			return true;
		} else {
			return false;
		}
	}

	public boolean lock(Transaction thistran) throws IOException {
		lock = LockType.Locked; // NULL

		intoBytes(thistran);

		while (!writeMyPage(thistran))
			;
		return true;
	}

	public boolean unLock(Transaction thistran) throws IOException {
		lock = LockType.Shared; // NULL

		intoBytes(thistran);
		while (!writeMyPage(thistran))
			;
		return true;
	}
}
