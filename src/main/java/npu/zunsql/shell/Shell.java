package npu.zunsql.shell;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import npu.zunsql.DBInstance;
import npu.zunsql.virenv.QueryResult;
import npu.zunsql.DBInstance;

public class Shell 
{
	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub
		//set multi-DBInstance
		DBInstance dbinstance[] = new DBInstance[10];
		String[] DB= new String[10];
	    String worked_DB = "";
		int DB_num = 0;
		String printwords = "zunSQL>";
		String commandlist = "";
		System.out.println("Enter \".help\" for usage hints.");
		while(true)
		{
		    System.out.print(printwords);
		    //input a CMD
		    Scanner scan = new Scanner(System.in);
		    String user_command = scan.nextLine();
		    //check enter
		    if(user_command.length() == 0)
		    {
		    	continue;
		    }
		    //analyse the user_command
		    switch(GetKeyword(user_command))
		    {
		        //.help
		        case ".help":
			   	    HelpInfor();
		            break;
		        //.open
		        case ".open":
		        	//find the DBName
		    	    String DBName = MakeCMD(user_command);
		    	    if(DBName.charAt(0) == ' ')
		    	    {
		    	    	System.out.println(DBName.substring(2));
		    	    	break;
		    	    }
		    	    if(CheckDBName(DBName, DB, DB_num) == -1)
		    	    {
		    	    	//DB is full
		    	    	if(DB_num == 10)
		    	    	{
		    	    		System.out.println("DB is full.");
		    	    		break;
		    	    	}
		    	    	DB[GetDBNo(DBName, DB)] = DBName;
			    	    DB_num++;
			    	    //open db
			    	    dbinstance[CheckDBName(DBName, DB, DB_num)] = DBInstance.Open(DBName);
		    	    }
		    	    worked_DB = DBName;
		    	    printwords = '[' + worked_DB + "]>";
		            break;
				//.close
		        case ".close":
		        	DBName = MakeCMD(user_command);
		        	if(DBName.charAt(0) == ' ')
		    	    {
		    	    	System.out.println(DBName.substring(2));
		    	    	break;
		    	    }
		        	int flag = CheckDBName(DBName, DB, DB_num);
		        	if(flag == -1)
		    	    {
		        		System.out.println(DBName + " is not opened.");
		    	    }
		        	else
		        	{
		        		dbinstance[flag].Close();
		        		DB[flag] = null;
		        		dbinstance[flag] = null;
			        	if(DBName.equals(worked_DB))
			        	{
			        		for(int i = 0;i < 10;i++)
			        		{
		        				worked_DB = DB[i];
			        			if(DB[i] != null)
			        			{
	                                break;
			        			}
			        		}
			        		if(worked_DB == null) 
			        		{
			        			printwords = "zunSQL>";
				        		worked_DB = "";
			        		}
			        		else
			        		{
				        		printwords = '[' + worked_DB + "]>";
			        		}
			        	}
		        	}
				    break;
				//.use
		        case ".use":
			        DBName = MakeCMD(user_command);
			        if(DBName.charAt(0) == ' ')
		    	    {
		    	    	System.out.println(DBName.substring(2));
		    	    	break;
		    	    }
			        if(CheckDBName(DBName, DB, DB_num)==-1)
		    	    {
			        	System.out.println(DBName + " is not opened.");
		    	    }
			        else
			        {
			        	worked_DB = DBName;
			        	printwords = '[' + worked_DB + "]>";
			        }
					break;
		        default:
			    //unmatched 
		        {
		        	if(user_command.charAt(0) == '.')
		        	{
		        		System.out.println(GetKeyword(user_command) + " is not defined.");
		        	}
		        	//SQL
		        	else
		        	{
		        		//check worked_DB
		        		if(worked_DB == "")
		        		{
		        			System.out.println("please open a database firstly.");
		        			break;
		        		}		        		
		        		//execute the SQL
		        		if(user_command.endsWith(";"))
		        		{
		        			commandlist = commandlist + user_command;
		        			String acommand = "";
		        			for(int i = 0; i < commandlist.length(); i++)
		        			{
		        				if(commandlist.charAt(i) == ';')
		        				{
		        					DoSQL(dbinstance[CheckDBName(worked_DB, DB, DB_num)],acommand);
		        					acommand = "";
		        				}
		        				else
		        				{
		        					acommand = acommand + commandlist.charAt(i);
		        				}
		        			}
		        			commandlist = "";
		        			printwords = '[' + worked_DB + "]>";
		        		}
		        		else
		        		{
		        			commandlist = commandlist + user_command;
		        			printwords = "....>";
		        			break;
		        		}
		            }
		            break;
		        }
		    }
		    
		}
	}
	
	private static void DoSQL(DBInstance dbInstance, String user_command) 
	{
		// TODO Auto-generated method stub
		QueryResult result;
		result = dbInstance.Execute(user_command);   
        if(result != null)
        {
        	//set the format of the table
        	if(user_command.toLowerCase().startsWith("select"))
        	{
        		List<String> head = new ArrayList<String>();
        		head = result.getHeaderString();
        		List<String> item = new ArrayList<String>();
        		for(int i = 0;i < head.size(); i++)
        		{
        			System.out.printf("%s\t",head.get(i));
        		}
        		System.out.print("\n");
        		List<List<String>> ResultList = new ArrayList<>();
        		ResultList = result.getRes();
        		for(int i = 0;i < ResultList.size(); i++)
        		{
        			item = ResultList.get(i);
        			for(int j = 0;j < item.size(); j++)
            		{
        				System.out.printf("%s\t",item.get(j));
            		}
        			System.out.print("\n");
        		}
        	}
        }
	}
	public static int GetDBNo(String DBName, String[] DB)
	{
		for(int i = 0;i < 10;i++)
		{
			if(DB[i] == null)
			{
			    return i;	
			}
		}
		return -1;
	}
	//CheckDBName
	public static int CheckDBName(String DBName, String[] DB, int DB_num) 
	{
		for(int i = 0;i < 10;i++)
		{
			if(DB[i]==null)
			{
				continue;
			}
			if(DB[i].equals(DBName))
			{
			    return i;	
			}
		}
		//if DBName is not in the DB return -1
		return -1;
	}
	//find the keywords in user_command
	public static String GetKeyword(String user_command)
	{
		if(user_command == "")
		{
			return "";
		}
		if(user_command.charAt(0) == '.')
		{
			String keywords = ""; 
			for(int i = 0;i < user_command.length();i++)
			{
				if(user_command.charAt(i) == ' ')
				{
					break;
				}
				keywords = keywords + user_command.charAt(i);
			}
			return keywords;
		}
		else
		{
			return user_command;
		}
	}
	public static String MakeCMD(String user_command) 
	{
	    String DBName = ""; 
	    int flag = 0;
	    int i;
		for(i = 0;i < user_command.length();i++)
		{
			if(flag == 1)
			{
				DBName = DBName + user_command.charAt(i);
			}
			if(user_command.charAt(i) == ' ')
			{
				flag++;
			}
		}
		if(flag != 1)
		{
			if(flag == 0)
			{
				DBName = " #The string name is empty.";
			}
			else
			{
				for(int j = user_command.length() - 1;j > user_command.length() - flag;j--)
				{
					if(user_command.charAt(j) !=' ')
					{
						DBName = " #DBName can not have ' '.";
						break;
					}
				}
				if(DBName.charAt(0) != ' ')
				{
					DBName = DBName.substring(0, DBName.length() - 1);
				}
			}
		}
		else
		{
			if(user_command.charAt(user_command.length() - 3) != '.'
					|| user_command.charAt(user_command.length() - 2) != 'd'
					|| user_command.charAt(user_command.length() - 1) != 'b')
			{
				DBName = " #\"" + DBName + "\" is incorrect.";
			}
		}
		return DBName; 
	}
	//information
	public static void HelpInfor() 
	{
		System.out.println(".open  *.db    Open a database or create a new database");
		System.out.println(".close *.db    Close a database");
		System.out.println(".use *.db      Update the * database to the current database");
		System.out.println(".help          Show this message");
	}
}
