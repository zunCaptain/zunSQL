package npu.zunsql.cache;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CacheMgr
{
    protected static final int CacheCapacity = 10;
    protected String dbName = null;
    protected static final int FILEHEADERSIZE = 1024;
    protected static final int UNUSEDLISTSIZE = 1024;

    //store the page
    protected List<Page> cacheList = null;
    //make the ID map the cache
    protected ConcurrentMap<Integer, Page> cachePageMap = null;
    //make the ID map the transaction
    protected Map<Integer, Transaction> transMgr = null;
    //record the transaction ID which has made change on the page
    protected Map<Integer, List<Page>> transOnPage = null;
    //record the number of the block which stores the unusedList_count
    protected List<Integer> unusedList_PageID = null; 
    private ReadWriteLock lock;

    public CacheMgr(String dbName)
    {
        this.dbName       = dbName;
        this.cacheList    = new ArrayList<Page>();
        this.cachePageMap = new ConcurrentHashMap<Integer, Page>();
        this.transMgr     = new HashMap<Integer, Transaction>();
        this.transOnPage  = new HashMap<Integer, List<Page>>();
        this.unusedList_PageID = new ArrayList<Integer>();
        this.lock = new ReentrantReadWriteLock();
    }

    public boolean isNew()
    {
        File db_file = new File(this.dbName);
        FileChannel fc = null;
        //if db_file has existed,use the API to read the file
        if(db_file.exists())
        {
            RandomAccessFile fin = null;
            try {
                fin = new RandomAccessFile(db_file, "rw");
                fc = fin.getChannel();
                ByteBuffer fileHeader = ByteBuffer.allocate(CacheMgr.FILEHEADERSIZE);
                fc.read(fileHeader, 0);

                int version=fileHeader.getInt(0);
                int magicNum=fileHeader.getInt(4);
                FileHeader obj = new FileHeader(version,magicNum);
                if(obj.isValid())
                {
                    Page.pageCount = fileHeader.getInt(8);
                	//System.out.println("This is pageCount:"+Page.pageCount);
                    ByteBuffer unusedListBuffer = ByteBuffer.allocate(CacheMgr.UNUSEDLISTSIZE);
                    fc.read(unusedListBuffer, CacheMgr.FILEHEADERSIZE);
                    unusedListBuffer.rewind();
                    //the first byte in the unusedListBuffer is used to store the usedListNum
                    int unusedListNum = unusedListBuffer.getInt();
                    for(int i = 0; i < unusedListNum; i++)
                    {
                    	//this if is used to move to the next unusedListBuffer when the last one is out of rage
                        if(i % 255 == 0)
                        {
                            unusedListBuffer.flip();   //set the current position to the zero
                            unusedListBuffer.getInt(); //Page.unusedID.size()
                            Page.unusedID.add(unusedListBuffer.getInt());
                        }
                        else if(i % 255 == 254)
                        {
                            Page.unusedID.add(unusedListBuffer.getInt());
                            int pageID = unusedListBuffer.getInt();
                            this.unusedList_PageID.add(pageID);
                            fc.read(unusedListBuffer, CacheMgr.FILEHEADERSIZE+CacheMgr.UNUSEDLISTSIZE+pageID*Page.PAGE_SIZE);
                        }
                        else
                            Page.unusedID.add(unusedListBuffer.getInt());
                    }
                    System.out.println("Load the database file successfully");
                    return false;
                }
                //version and magic is not right,so delete the file and create a new one
                else
                {
                    db_file.delete();
                    db_file.createNewFile();
                    fin = new RandomAccessFile(db_file, "rw");
                    fc = fin.getChannel();
                    fileHeader.rewind();
                    fileHeader.putInt(0, 1);        //version
                    fileHeader.putInt(4, 123);      //magic number
                    fc.write(fileHeader, 0);
                    return true;
                }

            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else
        {
            try {
                db_file.createNewFile();
                RandomAccessFile fin = null;
                fin = new RandomAccessFile(db_file, "rw");
                fc = fin.getChannel();
                ByteBuffer fileHeader = ByteBuffer.allocate(CacheMgr.FILEHEADERSIZE);
                fileHeader.rewind();
                fileHeader.putInt(0, 1);        //version
                fileHeader.putInt(4, 123);      //magic number
                fileHeader.rewind();
                System.out.println("Begin");
                //System.out.println("version:"+fileHeader.getInt(0));
                //System.out.println("magic:"+fileHeader.getInt(4));
                fc.write(fileHeader, 0);
                //System.out.println("This is ret:" + ret);
                System.out.println("Create new database successfully");
                fc.close();
                fin.close();
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        }
        return true;
    }

    public void close()
    {
        File db_file = new File(this.dbName);
        FileChannel fc = null;
        RandomAccessFile fin = null;
        try {
            fin = new RandomAccessFile(db_file, "rw");
            fc = fin.getChannel();
            //record Page.pagecount
            ByteBuffer fileHeader = ByteBuffer.allocate(CacheMgr.FILEHEADERSIZE);
            //read the block and write the pageCount to the third byte
            fc.read(fileHeader, 0);
            fileHeader.rewind();
            fileHeader.putInt(8, Page.pageCount);
            fc.write(fileHeader, 0);
            ByteBuffer unusedListBuffer = ByteBuffer.allocate(CacheMgr.UNUSEDLISTSIZE);
            int count = Page.unusedID.size();
            int unused_block_num = count/((CacheMgr.UNUSEDLISTSIZE/4)-2);
            int remain_block_num = count%((CacheMgr.UNUSEDLISTSIZE/4)-2);
            if(remain_block_num!=0){
            	unused_block_num = unused_block_num + 1;
            }
            else if(remain_block_num==0 && unused_block_num==0){
            	unused_block_num = 1;
            }
            while((unused_block_num-1)<this.unusedList_PageID.size()){
            	Page.unusedID.add(this.unusedList_PageID.get(this.unusedList_PageID.size()-1));
            	this.unusedList_PageID.remove(this.unusedList_PageID.size()-1);
            }
            int signal = 0;
            int history_block = -1;
            for(int i = 0; i < Page.unusedID.size(); i++)
            {
                if(i % 255 == 0)
                {
                    unusedListBuffer.rewind();
                    unusedListBuffer.putInt(Page.unusedID.size());
                    unusedListBuffer.putInt(Page.unusedID.get(i));
                }
                else if(i % 255 == 254 || i == Page.unusedID.size()-1)
                {
                	int backID;
                	if(this.unusedList_PageID.size()!=0){
                		backID = this.unusedList_PageID.get(0);
                		this.unusedList_PageID.remove(0);
                	}
                	else{
                		Page.pageCount = Page.pageCount + 1;
                		backID = Page.pageCount;
                		this.unusedList_PageID.add(backID);
                	}
                    unusedListBuffer.putInt(Page.unusedID.get(i));
                    unusedListBuffer.putInt(backID);
                    if(signal == 0){
                    	fc.write(unusedListBuffer, CacheMgr.FILEHEADERSIZE);
                    	signal = 1;
                    	history_block = backID;
                    }
                    else{
                    	fc.write(unusedListBuffer, CacheMgr.FILEHEADERSIZE+CacheMgr.UNUSEDLISTSIZE+history_block*Page.PAGE_SIZE);
                    	history_block = backID;
                    }
                }
                else{
                    unusedListBuffer.putInt(Page.unusedID.get(i));
                }
             }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * start a new transaction and return transID
     * new a objection and get the lock
     * record the transMgr
     **/
    public int beginTransation(String s)
    {
        Transaction trans = new Transaction(s, lock);
        trans.begin();
        this.transMgr.put(trans.transID, trans);
        return trans.transID;
    }

    /**commit the transaction and update the cache
	 * 1.get the transonPage
     * 2.read the page which will be changed and record it to the journal
     * 3.if hit cache , then update it
     * 4.write the new one to the page
     * 5.if not hit,then use LRU to replace one page
     */
    public boolean commitTransation(int transID) throws IOException {
        Transaction trans = transMgr.get(transID);
        if(trans.WR)
        {
            List<Page> writePageList = transOnPage.get(transID);
            File journal_file = new File(Integer.toString(transID)+"-journal");
            File db_file = new File(this.dbName);
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(journal_file));
            RandomAccessFile fin = new RandomAccessFile(journal_file, "rw");
            try{
	            FileChannel fc = fin.getChannel();
	            //the journal record the transaction ID and pageContent
	            ByteBuffer IDBuffer = ByteBuffer.allocate(Page.PAGE_SIZE+4);
	            if(writePageList != null)
	            {
	                for( int i = 0 ; i < writePageList.size() ; i++)
	                {
	                    Page copyPage = writePageList.get(i);
	                    Page tempPage = this.cachePageMap.get(copyPage.pageID);
	                    //cache not hit
	                    if (tempPage == null) {
	                        //cache is full , then use LRU to replace it
	                        if (this.cachePageMap.size() >= CacheMgr.CacheCapacity) {
	                            //delete the least use recently
	                            tempPage = this.cacheList.get(0);
	                            this.cacheList.remove(0);
	                            this.cachePageMap.remove(tempPage.pageID);
	                            //get Page from file , add it to cache list and map
	                            tempPage = getPageFromFile(copyPage.pageID);
	                        }
	                        //cache is not full
	                        else {
	                            //get Page from file , add it to cache list and map
	                            tempPage = getPageFromFile(copyPage.pageID);
	                        }
	                    }
	                    //cache hit then update list using LRU
	                    else {
	                        for (int j = 0; j < cacheList.size(); j++) {
	                            Page jPage = cacheList.get(j);
	                            if (jPage.pageID == copyPage.pageID) {
	                                cacheList.remove(j);
	                            }
	                        }
	                    }
	                    //tempPage is the original data , try to write journal
	                    if (journal_file.exists() && journal_file.isFile()) {
	                        //out.writeObject(tempPage);
	                        IDBuffer.rewind();
	                        IDBuffer.putInt(tempPage.pageID);
	                        int j;
	                        for(j=0;j<Page.PAGE_SIZE;j=j+4){
	                        	IDBuffer.putInt(tempPage.pageBuffer.getInt(j));
	                        }
	                        IDBuffer.flip();
	                        fc.write(IDBuffer,i*(Page.PAGE_SIZE+4));
                        }
                        else{
                            System.out.println("fail to write journal");
                        }
	
	                    byte [] copy_content = new byte[copyPage.getPageBuffer().limit()];
	                    copyPage.getPageBuffer().rewind();
	                    copyPage.getPageBuffer().get(copy_content);
	                    //System.out.println();
	                    //System.out.println("This is update file page:"+copy_content);
                        
                        //write cache
	                    tempPage.pageID = copyPage.pageID;
	                    tempPage.getPageBuffer().rewind();
	                    tempPage.pageBuffer.put(copy_content, 0, copy_content.length);
	
	                    byte [] temp_content = new byte[tempPage.getPageBuffer().limit()];
	                    tempPage.getPageBuffer().rewind();
	                    tempPage.getPageBuffer().get(temp_content);
	                    //System.out.println("This is second file page:"+ temp_content);
	
	                    this.cacheList.add(tempPage);
	                    this.cachePageMap.put(tempPage.pageID, tempPage);
	                    //write directly to the database file
	                    if (db_file.exists() && db_file.isFile()) {
	                        this.setPageToFile(tempPage, db_file);
	                    }
	                	}
	            	}
	        }finally{
	        	out.close();
	        	fin.close();
	        }
        }
//        this.transMgr.remove(transID);
//        this.transOnPage.remove(transID);
        trans.commit();
        return true;
    }

    /**
     * roll back the transaction
     * lease the lock and do not affect cache
     **/
    public boolean rollbackTransation(int transID)  {
    	System.out.println("This is transID:"+transID);
        Transaction trans = transMgr.get(transID);
        FileChannel fc = null;
        Page tempPage = null;
        if(trans.WR)
        {
            File journal_file = new File(Integer.toString(transID)+"-journal");
            System.out.println("This is file length:"+journal_file.length());
            int num = (int) (journal_file.length()/1028);
            int i=0;
            File db_file = new File(this.dbName);
            try{
            	RandomAccessFile fin = new RandomAccessFile(journal_file,"r");
            	fc = fin.getChannel();
            	FileLock lock = fc.lock(0, Long.MAX_VALUE, true);
            	ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
            	ByteBuffer tempBuffer_ID = ByteBuffer.allocate(4);
            	for(i=0;i<num;i++){
            		fc.read(tempBuffer_ID,i*1028);
            		fc.read(tempBuffer,i*1028+4);
                	int tmp_ID = tempBuffer_ID.getInt(0);
                	tempPage = new Page(tmp_ID,tempBuffer);
                	Page incachePage = this.cachePageMap.get(tmp_ID);
                	if(incachePage != null){
                		incachePage.pageBuffer.put(tempPage.pageBuffer);
                	}
                	if(db_file.exists() && db_file.isFile()){
                		this.setPageToFile(tempPage, db_file);
                	}
                	tempBuffer_ID.clear();
                	tempBuffer.clear();
            	}
            	lock.release();
            	fin.close();
            }
            catch(IOException e) {
                e.printStackTrace();
            }
            finally{
            	
            }
            //try
            //{
//                while(.available() > 1)
//                {
//                    Page srcPage = (Page)in.readObject();
//                    Page tempPage = this.cachePageMap.get(srcPage.pageID);
//                    //cache hit and update the cache file
//                    if (tempPage != null)
//                    {
//                        tempPage.pageBuffer.put(srcPage.pageBuffer);
//                    }
//                    //at the same time , write this into the database file 
//                    //no matter cache is hit , always write into the database file
//                    if (db_file.exists() && db_file.isFile()) {
//                        this.setPageToFile(srcPage, db_file);
//                    }
//                }
//            }
//            catch(Exception e)
//            {
//                e.printStackTrace();
//            }
//            finally
//            {
//                if(in != null) try {
//                    in.close();
//                }
//                catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
        }
        trans.rollback();
//      this.transMgr.remove(transID);
        return true;
    }

    /**use transID to read pageID and return page_copy
     * if this page has been stored in the cacheList ,then directly return it
     * else try to get it from file:
     * 		if cacheList is full , use LRU to replace one page
     * 		else , add this page into cacheList
     **/
    public Page readPage(int transID, int pageID)
    {

        Page tempPage = null;
        tempPage = this.cachePageMap.get(pageID);

        List<Page> writePageList= transOnPage.get(transID);
        
        //test if this page has been written by this transaction but not commit
        if(writePageList != null)
        {
            for( int i = 0 ; i < writePageList.size() ; i++)
            {
                Page copyPage = writePageList.get(i);
                if(copyPage.pageID == pageID)
                    return copyPage;
            }
        }

        //cache not hit
        if(tempPage == null)
        {
            //cache has been full , use LRU to replace it
            if(this.cachePageMap.size() >= CacheMgr.CacheCapacity)
            {
                tempPage = this.cacheList.get(0);
                this.cacheList.remove(0);
                this.cachePageMap.remove(tempPage.pageID);

                tempPage = getPageFromFile(pageID);
                this.cacheList.add(tempPage);
                this.cachePageMap.put(tempPage.pageID, tempPage);
            }
            else
            {
                tempPage = getPageFromFile(pageID);
                this.cacheList.add(tempPage);
                this.cachePageMap.put(tempPage.pageID, tempPage);
            }

        }
        //cache hit
        else
        {
            //use LRU to update the cacheList
            for( int j = 0 ; j < cacheList.size() ; j++)
            {
                Page jPage = cacheList.get(j);
                if (jPage.pageID == tempPage.pageID)
                {
                    cacheList.remove(jPage);
                }
            }
            this.cacheList.add(tempPage);
        }
        return tempPage;
    }

    /**
     *use transID to get the write queue and add it to writePagelist
     *Attention:one transaction can write more than one page,so we should use a list to store it
     **/
    public boolean writePage(int transID, Page tempBuffer)
    {
        List<Page> writePageList= transOnPage.get(transID);
        if(writePageList == null)
        {
            writePageList = new ArrayList<Page>();
            writePageList.add(tempBuffer);
            transOnPage.put(transID, writePageList);
        }
        else
        {
            writePageList.add(tempBuffer);
        }
        return true;
    }

    /**
     * Add the unused pageID into the Page.unusedID list
     **/
    public void deletePage(int transID, int pageID)
    {
        Page.unusedID.add(pageID);
    }


    /**
     *write page to file
     **/
    private boolean setPageToFile(Page tempPage, File file)
    {
        FileChannel fc = null;
        try
        {
            if(!file.exists())
            {
                file.createNewFile();
            }
            RandomAccessFile fin = new RandomAccessFile(file, "rw");
            fc = fin.getChannel();
            //write lock
            FileLock lock = fc.lock();
            tempPage.pageBuffer.flip();
            fc.write(tempPage.pageBuffer,CacheMgr.FILEHEADERSIZE+CacheMgr.UNUSEDLISTSIZE+tempPage.pageID*Page.PAGE_SIZE);
            lock.release();
        	fin.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                if(fc != null)
                {
                    fc.close();
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        return true;
    }


    /**
     *Read the Page  from the file using pageID
     **/
    private Page getPageFromFile(int pageID)
    {
        Page tempPage = null;
        FileChannel fc = null;
        try
        {
            File file = new File(this.dbName);
            RandomAccessFile fin = new RandomAccessFile(file, "rw");
            fc = fin.getChannel();
            //share lock
            FileLock lock = fc.lock(0, Long.MAX_VALUE, true);
            ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
            fc.read(tempBuffer, CacheMgr.FILEHEADERSIZE+CacheMgr.UNUSEDLISTSIZE+pageID*Page.PAGE_SIZE);
            tempPage = new Page(pageID, tempBuffer);
            lock.release();
            fin.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                if(fc != null)
                {
                    fc.close();
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }
        }
        return tempPage;
    }
}
