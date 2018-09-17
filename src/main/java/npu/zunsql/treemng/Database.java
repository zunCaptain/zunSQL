package npu.zunsql.tree;

import npu.zunsql.cache.Page;
import  npu.zunsql.cache.CacheMgr;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ed on 2017/10/29.
 */
public class Database
{
    //表示dataBase的名字
    private String dBName;

    //系统表,用于存储表名和对应pageID
    private Table master;

    // page层的Mgr，用于对Page层进行操作。
    private CacheMgr cacheManager;

    // 根据dbname构造数据库缓存对象
    public Database(String name) throws IOException, ClassNotFoundException
    {
        dBName = name;
        cacheManager = new CacheMgr(dBName);

        // 判断数据库是否为新建数据库，即pageid为0的一页是否被填充
        boolean dbisNew = false;
        dbisNew=cacheManager.isNew();
        if(!dbisNew)
        {
            Transaction initTran = beginReadTrans();

            //从page[0]解析master。
            master = new Table(0, cacheManager, initTran);
            initTran.Commit();
        }
        else
        {
            //建立masterTable
            Transaction initTran = beginWriteTrans();
            addMaster(initTran);
            initTran.Commit();
        }
    }

    // 构造一个系统表
    private boolean addMaster(Transaction initTran) throws IOException, ClassNotFoundException {
        // 添加master table
        Column keyColumn = new Column(BasicType.String,"tableName",0);
        Column valueColumn = new Column(BasicType.Integer,"pageNumber",1);
        List<String> sList = new ArrayList<String>();
        List<BasicType> tList = new ArrayList<BasicType>();
        sList.add("tableName");
        sList.add("pageNumber");
        tList.add(BasicType.String);
        tList.add(BasicType.Integer);
        master = createTable("master","tableName",sList,tList,initTran);
        return true;
    }

    // 关闭数据库
    public boolean close()
    {
        cacheManager.close();
        return true;
    }


    //开始一个读事务操作
    public Transaction beginReadTrans()
    {
        return new ReadTran(cacheManager.beginTransation("r"),cacheManager);
    }

    //开始一个写事务
    public Transaction beginWriteTrans()
    {
        return new WriteTran(cacheManager.beginTransation("w"),cacheManager);
    }

    //根据传来的表名，主键以及其他的列名来新建一个表
    public Table createTable(String tableName, String keyName, List<String> columnNameList,List<BasicType> tList, Transaction thisTran) throws IOException, ClassNotFoundException
    {
        ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);

        byte [] bytes=new byte[Page.PAGE_SIZE] ;
        ByteArrayOutputStream byt=new ByteArrayOutputStream();

        // 将表头信息和首节点信息存入ByteBuffer中 新建的表锁应该为什么锁
        LockType lock=LockType.Shared;

        List<Column> columns = new ArrayList<Column>();

        // 整合columnlist并且将主键放置第一列
        for(int i=0;i<columnNameList.size();i++)
        {
            int temp = 0;
            if(columnNameList.get(i).equals(keyName))
            {
                Column tempColumn = new Column(tList.get(i),columnNameList.get(i),0);
                columns.add(0,tempColumn);
                temp--;
            }
            else
            {
                Column tempColumn = new Column(tList.get(i),columnNameList.get(i),i+1+temp);
                columns.add(tempColumn);
            }
        }

        ObjectOutputStream obj=new ObjectOutputStream(byt);
        obj.writeObject(tableName);

        obj.writeObject(columns.get(0));
        obj.writeObject(columns);

        obj.writeObject(lock);
        obj.writeObject(-1);
        bytes=byt.toByteArray();
        tempBuffer.put(bytes);

        Page tablePage = new Page(tempBuffer);
        byte [] bytess=new byte[Page.PAGE_SIZE] ;
        tempBuffer.rewind();
        tablePage.getPageBuffer().get(bytess,0, tablePage.getPageBuffer().remaining());


        cacheManager.writePage(thisTran.tranNum,tablePage);


        //thisTran.Commit();
        Integer pageID = tablePage.getPageID();



        /*
        Page mpage = cacheManager.readPage(thisTran.tranNum, tablePage.getPageID());
        byte [] bytess=new byte[Page.PAGE_SIZE] ;
        mpage.getPageBuffer().get(bytess,0,mpage.getPageBuffer().remaining());
        */
        /*
        //test/*
        ByteBuffer tBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
        tBuffer.putInt(0, 123);
        Page tpage = new Page(tBuffer);
        tpage.getPageBuffer().rewind();
        int t = tpage.getPageBuffer().getInt(0);
        cacheManager.writePage(thisTran.tranNum, tpage);
        thisTran.Commit();
        Page mpage = cacheManager.readPage(thisTran.tranNum, tpage.getPageID());
        int ttt = mpage.getPageBuffer().getInt(0);

        /* 当表不是master表时，对master表进行插入 */
        if(!tableName.equals("master"))
        {
            List<String> masterRow_s = new ArrayList<String>();
            masterRow_s.add(tableName);
            masterRow_s.add(pageID.toString());

            Cursor masterCursor = master.createCursor(thisTran);
            masterCursor.insert(thisTran, masterRow_s);
        }
        return  new Table(pageID,cacheManager,thisTran);  //NULL
    }

    //根据传来的表名，主键以及其他的列名来新建一个表放入tableList中
    public View createView(List<String> sList, List<BasicType> tList, List<List<String>> rowStringList, Transaction thisTran)
    {
        return new View(sList,tList,rowStringList);
    }

    // 删除一张表
    public boolean dropTable(String tableName,Transaction thisTran) throws IOException, ClassNotFoundException
    {
        Cursor masterCursor = master.createCursor(thisTran);
        masterCursor.moveToUnpacked(thisTran,tableName);
        int pageID = masterCursor.getCell_i("pageNumber");
        Table thistable = new Table(pageID,cacheManager,thisTran);
        thistable.getRootNode(thisTran).drop(thisTran);
        cacheManager.deletePage(thisTran.tranNum,pageID);
        masterCursor.delete(thisTran);
        return true;
    }

    // 删除一张表
    public boolean dropTable(Table table,Transaction thisTran) throws IOException, ClassNotFoundException
    {
        return dropTable(table.tableName,thisTran);
    }


    //根据传来的表名返回Table表对象
    public Table getTable(String tableName, Transaction thisTran) throws IOException, ClassNotFoundException
    {
        Cursor masterCursor = master.createCursor(thisTran);
        masterCursor.moveToUnpacked(thisTran,tableName);
        return new Table(masterCursor.getCell_i("pageNumber"),cacheManager,thisTran);
    }

    //给整个数据库中的表全部加锁
    public boolean lock(Transaction thisTran) throws IOException, ClassNotFoundException
    {
        if(master.isLocked())
        {
            return false;
        }
        else
        {
            Cursor masterCursor = master.createCursor(thisTran);
            do
            {
                Table temp = new Table(masterCursor.getCell_i("pageNumber"),cacheManager,thisTran);
                temp.lock(thisTran);
            }while(masterCursor.moveToNext(thisTran));
            master.lock(thisTran);
            return true;
        }
    }

    //给数据库中全部的表解锁
    public boolean unLock(Transaction thisTran) throws IOException, ClassNotFoundException
    {
        if(master.isLocked())
        {
            master.unLock(thisTran);
            Cursor masterCursor = master.createCursor(thisTran);
            do
            {
                Table temp = new Table(masterCursor.getCell_i("pageNumber"),cacheManager,thisTran);
                temp.unLock(thisTran);
            }while(masterCursor.moveToNext(thisTran));
            return true;
        }
        else
        {
            return true;
        }
    }

}
