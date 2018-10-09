package npu.zunsql;
import npu.zunsql.DBInstance;
import npu.zunsql.cache.CacheMgr;
import npu.zunsql.cache.Page;
import npu.zunsql.cache.Transaction;
import npu.zunsql.virenv.QueryResult;
import sun.misc.Cache;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args)
    {
        /* test for readPage and writePage in cacheMgr, when a transation commit
        CacheMgr cacheManager = new CacheMgr("student");
        int tranID  = cacheManager.beginTransation("w");

        ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
        tempBuffer.putInt(0,987);
        Page tempPage = new Page(tempBuffer);
        cacheManager.writePage(tranID, tempPage);
        try
        {
            cacheManager.commitTransation(tranID);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        Page rPage = cacheManager.readPage(2, tempPage.getPageID());
        int ret = rPage.getPageBuffer().getInt(0);
        System.out.println(ret);

        */

        DBInstance dbinstance = DBInstance.Open("test.db");
        QueryResult result;
        result = dbinstance.Execute("create table student(stuno int primary key, name varchar,score double)");
        result = dbinstance.Execute("create table teacher(stuno int primary key, name varchar,score double)");
        
        //result = dbinstance.Execute("select * from student");


        //result = dbinstance.Execute("insert into student (stuno, name, score) values (2017006, 'zhang', 98.0+1)");
        result = dbinstance.Execute("insert into teacher (stuno, name, score) values (2017005, 'zhang', 98.0+1)");
        
//        result = dbinstance.Execute("drop table teacher");
        
        result = dbinstance.Execute("select * from student");
        System.out.println(result.getRes());
        
        result = dbinstance.Execute("select * from teacher");
        System.out.println(result.getRes());
        
        result = dbinstance.Execute("select * from master");
        System.out.println(result.getRes());
        
        result = dbinstance.Execute("drop table student");
//        System.out.println(result.getRes());
        
        result = dbinstance.Execute("select * from master");
        System.out.println(result.getRes());

        //System.out.println("Insert Row:" + result.getAffectedCount());
        //result=dbinstance.Execute("select * from student");
        System.out.println("select Row:"+result.getAffectedCount());
        dbinstance.Close();
    }
}
