## 读写事务
### #define TT_READ 1
### #define TT_WRITE 2
## 数据类型
### #define CT_INT 1
### #define CT_DOUBLE 2
### #define CT_STRING 3
## 二级锁
### #define LO_LOCKED 1
### #define LO_SHARED 2

# class Database
## private:
### String DataBaseName;
	数据库名，DataBaseName仅在初始化时被赋值，其后不做任何修改。

### List`<Table>` TableList;
	表的列表

### private Page pageOne;
    Mgr页放在第一页,内容包括：database的对应页
   
### private CacheMgr cacheManager;
    page层的Mgr，用于对Page层进行操作。
    
    
## public：
### Database(String DBName);
	初始化数据库的名称

### Transaction BeginTrans(int TransType);
	开始一个事务,TransType表示事务类型
	读事务：TT_READ，写事务：TT_WRITE
	成功返回Transaction对象，失败返回0

### Table CreateTable(String TableName，List`<Column>` ColumnList);
	添加一张表
	成功返回Table，失败返回空。

### Table GetTable(String TableName);
	根据表名得到一张表
	成功返回Table，失败返回空。

### bool Lock();
	给整个数据库加锁
	遍历整个TableList，对所有的表进行加锁。
	成功返回true，失败返回false

### bool UnLock();
	给整个数据库解锁
	遍历整个TableList，对所有的表进行解锁。
	成功返回true，失败返回false




# class Transaction
## private:
### int tranNum;
	事务编号，从下层获取，用于标定不同事务。
	在本类中，tranNum仅在初始化时被赋值，其后不做任何修改。

## public：
### Transaction(int tranNum)
	本类的构造函数,主要实现对属性的初始化赋值。

### bool Commit();
	提交事务
	成功返回true，失败返回false

### bool Rollback();
	回滚事务
	成功返回true，失败返回false





# class Table
## private:
### String TableName;
	表名
### Column keyColumn;
	主键
### List`<Column>` columns;
	所有列
### final static int LO_LOCKED = 1;	
	写锁：LO_LOCKED
### final static int LO_SHARED = 2;	
	读锁：LO_SHARED
### private Integer lock;
	本变量为锁标记，主要用于标记排他写。
### Node rootNode;
    根结点。

## protected
### Node getRootNode();
    得到根结点。

## public:
### Table(String name,Column key,List`<Column>` colist)；
	本函数为table类的构造函数
	TName为TableName,KeyColumn为Key,OtherColumn为其他列

### boolean Drop();
	删除一张表
	成功返回true，失败返回false。

### boolean Clear();
	清空一张表
	成功返回true，失败返回false。

### String GetTableName();
	得到表名
	成功返回TableName， 失败返回空。
	
### boolean isLocked();
	返回是否被锁。
### boolean getLock()；
    返回是否被锁。
### boolean lock()；
    上锁。
### boolean unLock()；
    解锁
### Cursor CreateCursor(String TableName);
	添加一个光标
	成功返回cursor，失败返回空。
### Column getKeyColumn()；
    得到主键。
### List`<Column>`getColumns;
    得到所有列。
### Column getColumn(String columnName)；
    得到指定列。



# class Row
## private:
### List`<Cell>` Celllist;
    一条记录，数据库中的一行数据。
### Cell keyCell;
    主键。
### Row LeftBrotherRow;
    除了最小节点外，其他节点都拥有他的左节点
### Row RightBrotherRow;
    除了最大节点外，其他节点都拥有他的右节点

## public:
### Row(List`<Cell>` Celllist);
	Row类的构造函数
### boolean setLeftRow(Row row);
    设置左结点。
### Row getLeftRow();
    得到左结点。
### boolean setRightRow(Row row);
    设置右结点。
### Row getRightRow();
    得到右结点。
### bool ChangeCell(Cell ThisCell);
	改变某单元的值。
    成功返回true，失败返回false。
### Cell getCell(Column ThisColumn);
	根据列信息找到单元格。
### List<Cell> getCellList();
    得到所有单元格。
### Cell getKeyCell();
    得到主键。
### boolean isMatch(Column key,List<Column> others);
    判断两行是否相等。



# class Cell
## Private:
### Column ThisColumn;
	用于表征Cell所属的列。
### Integer value;
	若为整形，则使用value进行存取
### Double dValue;
	若为浮点型形，则使用dvalue进行存储
### String SValue;
	若为字符串，则使用Svalue进行存取
	
## Public:
### Cell(Column ThisColumn,int ThisValue);
    Integer类型的构造函数。
### Cell(Column ThisColumn,double ThisValue);
    double类型的构造函数。
### Cell(Column ThisColumn,String Thisvalue);
    string类型的构造函数。
### Column getColumn();
    返回该Cell对应的Column。
### int getvalue_int();
    返回对应的Integer的值。
### double getvalue_double();
    返回对应的double的值。
### String getvalue_String();
    返回对应的string的值。
### public boolean equalTo(Cell cell);
    两个Cell是否相等。
### public boolean bigerThan(Cell cell);
    两个Cell的大小比较。
### public String getType();
    返回Cell对应列的类型。


# class Column
## private:
### Integer columnType;
    列的类型，1为Integer,2为Float,3为string;
### String ColumnName;
    列的名字。
    
## public:
### Column(int Type,String ColumnName)
    构造函数。
### boolean isMatch(Column column);
    判断两列是否为同一列。
### boolean isEqual(Cell a,Cell b);
    判断两个cell是否相等。
### boolean isBigger(Cell a,Cell b);
    比较两个cell的大小。
### String getType();
    返回Column的类型。
### String getName();
    返回Column的名字，即列名。
### String getColumnName()；
    返回Column的名字，即列名。


# class Cursor
## private:
### Table aimTable;
	表示目标表，在构造函数中赋值。
### Row thisRow;
    游标指向的行。

## public：
### Cursor(Table thisTable)
	本函数为cursor类的构造函数.

### bool MovetoFirst()
	本函数将Cursor指向Btree的第一个元素。
	成功返回true，失败返回false。

### bool MovetoLast()
	本函数将Cursor指向Btree的最后一个元素。
	成功返回true，失败返回false。

### bool MovetoNext()
	本函数将Cursor指向Cursor指向的下一个元素。
	成功返回true，失败返回false。

### bool MovetoPrevious()
	本函数将Cursor指向Cursor指向的上一个元素。
	成功返回true，失败返回false。

### bool MovetoUnpacked(Cell keyCell)
	本函数用于将Cursor定位到指定key的位置，如果匹配不到，则将Cursor停在与key值相近的某位置
	成功返回true，失败返回false。

### boolean Delete(Transaction thistran);
	删除节点
	成功返回true，失败返回false。

### boolean Insert(Transaction thistran,Row row)；
	插入节点
	成功返回true，失败返回false。

### Integer GetKeySize(Transaction thistran)；
	获取KeySize
	成功返回true，失败返回false。

### Cell GetKey(Transaction thistran)；
	获取Key值
	成功返回true，失败返回false。

### Integer GetDataSize(Transaction thistran)；
	获取数据大小
	成功返回true，失败返回false。

### Row GetData(Transaction thistran);
	获取数据
	成功返回data，失败返回空。

### boolean setData(Transaction thistran,Row row)；
	修改数据
	成功返回true，失败返回false。
	
# class Node
## private
### List<Row> rowList;
    每个节点包含不少于M/2，不超过M的Row。
### List<Node> sonList;
    每个节点包含不少于M/2+1，不超过M+1的SonNode。
### Node father;
    表示父亲节点.
### int order;
    表示本节点在父亲节点儿子中的第几位。
### Node(List<Row> thisRowList, List<Node> thisSonList, int thisOrder);
    根据Node的属性构造Node。
### Node devideNode()；
    分裂除根节点外的其他节点。
### boolean rootDevideNode()；
    分裂根节点。
### boolean adjustNode(int sonOrder)；
    调整本节点使其顺序为sonOrder的儿子row数量恢复至M/2。
### boolean addNode(Row row, Node node)；
    在本节点中添加子节点，分别添加row和对应的SonNode。
### boolean deleteNode(int sonOrder)；
    删除指定子结点。
### boolean insertRow(Row row)；
    插入一条记录（Row）。
### boolean deleteRow(Cell key);
    删除一条记录。
### Row getRow()；
    得到行。
### Row getFirstRow()；
    得到第一行。
### Row getLastRow()；
    得到最后一行。
### Row getSpecifyRow(Cell key)；
    得到指定行。


## protected
### Node();
    不对外开放的node的简单构造.

## public
### final static int M = 3;
    b树的阶数