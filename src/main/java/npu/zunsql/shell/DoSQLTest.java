package npu.zunsql.shell.test;

import java.io.File;  
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.BufferedReader;  
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileOutputStream;  
import java.io.PrintStream; 

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import npu.zunsql.DBInstance;
import npu.zunsql.shell.Shell;

public class DoSQLTest {
	
	@Test
	public void DoSQLTest() throws Exception {
		DBInstance dbinstance ;
		dbinstance = DBInstance.Open("Test.db");
		Shell shell = new Shell();
		String pathname1 = "C:\\Users\\Administrator\\eclipse-workspace\\zunSQL\\trunk\\src\\main\\java\\npu\\zunsql\\shell\\test";
		String pathname2 = "C:\\Users\\Administrator\\eclipse-workspace\\zunSQL\\trunk\\src\\main\\java\\npu\\zunsql\\shell\\answer";
		String pathname3 = "C:\\Users\\Administrator\\eclipse-workspace\\zunSQL\\trunk\\src\\main\\java\\npu\\zunsql\\shell\\output";		
		File file1 = new File(pathname1);
        File file2 = new File(pathname2);     
        File file3 = new File(pathname3);
        String [] filename1 = file1.list();
        String [] filename2 = file2.list();

        for( int i = 0; filename1[i] != null; i++)
        {
        	int num = 0;
            if( filename1[i].substring(filename1[i].length()-3,filename1[i].length()).equals("txt") )
            {
            	num = num + 1;
            	for( int j = 0; filename2[j] != null; j++)
                {
            		if(filename1[i].equals(filename2[j]) )
            		{
            			File txt1 = new File(pathname1+"/"+filename1[i]);
            			File txt2 = new File(pathname2+"/"+filename2[j]);
            			File txt3 = new File(pathname3+"/"+filename1[i]);
            			if(!txt3.exists())
            			{
            				txt3.createNewFile();
            			}	
            			BufferedReader br1 = new BufferedReader(new FileReader(txt1)); // 建立一个对象，它把文件内容转成计算机能读懂的语言  
            	        BufferedReader br2 = new BufferedReader(new FileReader(txt2));
            	        BufferedReader br3 = new BufferedReader(new FileReader(txt3));         
            	        PrintStream ps = new PrintStream(txt3);         
            	        String line = br1.readLine(); 
            	        while (line != null) 
            	        { 
            	        	System.setOut(ps);
            	        	shell.DoSQL(dbinstance, line);                   
            	        	line = br1.readLine();
            	        } // 一次读入一行数据  
            	        String line2 = br2.readLine(); 
            	        String line3 = br3.readLine();
            	        while(line2 != null && line3 != null)
            	        {
            	        	assertEquals(line3, line2);
            	        	line2 = br2.readLine();
            	        	line3 = br3.readLine();
            	        }
            	        assertEquals(line2, null);
            	        assertEquals(line3, null);
            	        br1.close();
            	        br2.close();
            	        br3.close();
            		}
            	}
            }
        }
        dbinstance.Close();
        String pathname = "C:\\Users\\Administrator\\eclipse-workspace\\zunSQL\\trunk\\src\\main\\java\\npu\\zunsql\\shell\\Test.db";
        File file = new File(pathname);
        file.delete();
	}
}
