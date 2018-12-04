package npu.zunsql;

import npu.zunsql.DBInstance;
import npu.zunsql.cache.CacheMgr;
import npu.zunsql.cache.Page;
import npu.zunsql.cache.Transaction;
import npu.zunsql.virenv.QueryResult;
//import sun.misc.Cache;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Hello world!
 */
public class App {
	public static void main(String[] args) {
		/*
		 * test for readPage and writePage in cacheMgr, when a transation commit
		 * CacheMgr cacheManager = new CacheMgr("student"); int tranID =
		 * cacheManager.beginTransation("w");
		 * 
		 * ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
		 * tempBuffer.putInt(0,987); Page tempPage = new Page(tempBuffer);
		 * cacheManager.writePage(tranID, tempPage); try {
		 * cacheManager.commitTransation(tranID); } catch (IOException e) {
		 * e.printStackTrace(); } Page rPage = cacheManager.readPage(2,
		 * tempPage.getPageID()); int ret = rPage.getPageBuffer().getInt(0);
		 * System.out.println(ret);
		 * 
		 */

		DBInstance dbinstance = DBInstance.Open("test.db", 5);
		QueryResult result;
		
		//result = dbinstance.Execute("begin transaction");
		result = dbinstance.Execute("create table student(stuno int primary key, sname varchar,score double,course varchar)");
		//result = dbinstance.Execute("create table teacher(teano int primary key, tname varchar,Tcourse varchar)");
		
		//result = dbinstance.Execute("create table courseTable(courseName varchar primary key, courseNumber int)");

		//result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017005, 'zhang', 98.0+1, 'OS')");
		//result = dbinstance.Execute("insert into student (stuno, sname, score, course) values (2017004, 'li', 80, 'DS')");		result = dbinstance.Execute("insert into student (stuno, name, score) values (2017004, 'li', 66)");
		//result = dbinstance.Execute("insert into teacher(teano, tname ,Tcourse) values (2017004, 'jun',  'DS')");		
		//result = dbinstance.Execute("insert into teacher(teano, tname ,Tcourse) values (2017006, 'zhang',  'c')");
		//result = dbinstance.Execute("insert into courseTable(courseName,courseNumber) values ('OS',1024)");
		//result = dbinstance.Execute("commit");
		//result = dbinstance.Execute("select courseNumber from courseTable");
		//System.out.println(result.getRes());

		//result = dbinstance.Execute("create table student(stuno int primary key, name varchar ,score double,course varchar)");
		///result = dbinstance.Execute("insert into student (stuno, name, score, course) values (2017006, 'ZHANG', 95, 'KS')");
		//result = dbinstance.Execute("insert into student (stuno, name, score, course) values (2017004, 'LI', 91, 'DOS')");				
		//result = dbinstance.Execute("update student set score=666 where stuno=2017005");
		
		//result = dbinstance.Execute("select * from student where score>80 ");
		//System.out.println("select * where name =zhang:   "+result.getRes());
		//result = dbinstance.Execute("select stuno,score from student where stuno='2017004' and name ='li'");
		
		//result = dbinstance.Execute("begin transaction");
		
//		result = dbinstance.Execute("select sname,course,courseNumber from student,teacher,courseTable where sname=tname and course=courseName");
//		System.out.println(result.getRes());
		
		//result = dbinstance.Execute("select courseNumber from courseTable");
		//System.out.println(result.getRes());
		
		//result = dbinstance.Execute("insert into courseTable(courseName,courseNumber) values ('JK',2048)");
		
		//result = dbinstance.Execute("select courseNumber from courseTable");
		//System.out.println(result.getRes());
		
		//result = dbinstance.Execute("rollback");
		
		//result = dbinstance.Execute("select courseNumber from courseTable");
		//System.out.println(result.getRes());
		
//		result = dbinstance.Execute("delete from student");
//		
//		result = dbinstance.Execute("select * from student");
//		System.out.println(result.getRes());

//		result = dbinstance.Execute("update student set score=666 where stuno=2017005");
//		
//		result = dbinstance.Execute("rollback");
//		
//		result = dbinstance.Execute("select * from student");
		

   		dbinstance.Close();
	}
}
