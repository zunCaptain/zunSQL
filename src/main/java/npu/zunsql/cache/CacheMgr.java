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


    //缓存页链表，按照LRU策略组织顺序
    protected List<Page> cacheList = null;
    //缓存页字典
    protected ConcurrentMap<Integer, Page> cachePageMap = null;
    //事务管理器
    protected Map<Integer, Transaction> transMgr = null;
    //事务写操作对应的页
    protected Map<Integer, List<Page>> transOnPage = null;
    private ReadWriteLock lock;


    public CacheMgr(String dbName)
    {
        this.dbName       = dbName;

        this.cacheList    = new ArrayList<Page>();
        this.cachePageMap = new ConcurrentHashMap<Integer, Page>();
        this.transMgr     = new HashMap<Integer, Transaction>();

        this.transOnPage  = new HashMap<Integer, List<Page>>();
        this.lock = new ReentrantReadWriteLock();
    }

    public boolean isNew()
    {
        File db_file = new File(this.dbName);
        FileChannel fc = null;
        if(db_file.exists())
        {
            RandomAccessFile fin = null;
            try {
                fin = new RandomAccessFile(db_file, "rw");
                fc = fin.getChannel();
                ByteBuffer fileHeader = ByteBuffer.allocate(this.FILEHEADERSIZE);
                fc.read(fileHeader, 0);

                int version =  fileHeader.getInt(0);
                int magicNum =  fileHeader.getInt(4);
                if(version == 1 && magicNum == 123)
                {
                    Page.pageCount = fileHeader.getInt(8);

                    ByteBuffer unusedListBuffer = ByteBuffer.allocate(this.UNUSEDLISTSIZE);
                    fc.read(unusedListBuffer, this.FILEHEADERSIZE);
                    unusedListBuffer.rewind();
                    int usedListNum = unusedListBuffer.getInt();
                    for(int i = 0; i < usedListNum; i++)
                    {
                        if(i % 254 == 0)
                        {
                            unusedListBuffer.flip();
                            unusedListBuffer.getInt();          //Page.unusedID.size()
                            Page.unusedID.add(unusedListBuffer.getInt());
                        }
                        else if(i % 254 == 253)
                        {
                            Page.unusedID.add(unusedListBuffer.getInt());
                            int pageID = unusedListBuffer.getInt();
                            fc.read(unusedListBuffer, this.FILEHEADERSIZE+this.UNUSEDLISTSIZE+pageID*Page.PAGE_SIZE);
                        }
                        else
                            Page.unusedID.add(unusedListBuffer.getInt());
                    }
                    return false;
                }
                else
                {
                    db_file.delete();
                    db_file.createNewFile();
                    fin = new RandomAccessFile(db_file, "rw");
                    fc = fin.getChannel();
                    fileHeader.rewind();
                    fileHeader.putInt(0, 1);           //version
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
                ByteBuffer fileHeader = ByteBuffer.allocate(this.FILEHEADERSIZE);
                fileHeader.rewind();
                fileHeader.putInt(0, 1);           //version
                fileHeader.putInt(4, 123);      //magic number
                fileHeader.rewind();
                System.out.println("begin");
                System.out.println(fileHeader.getInt(0));
                System.out.println(fileHeader.getInt(4));
                int ret = fc.write(fileHeader, 0);
                System.out.println(ret);
                System.out.println("end");
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

            //保存当前pageCount
            ByteBuffer fileHeader = ByteBuffer.allocate(this.FILEHEADERSIZE);
            fc.read(fileHeader, 0);
            fileHeader.rewind();
            fileHeader.putInt(8, Page.pageCount);
            fc.write(fileHeader, 0);

            ByteBuffer unusedListBuffer = ByteBuffer.allocate(this.UNUSEDLISTSIZE);
            for(int i = 0; i < Page.unusedID.size(); i++)
            {
                // 0    255
                // 256  511
                // 512  767
                // 768
                if(i % 254 == 0)
                {
                    unusedListBuffer.rewind();
                    unusedListBuffer.putInt(Page.unusedID.size());
                    unusedListBuffer.putInt(Page.unusedID.get(i));
                }
                else if(i % 254 == 253 || i == Page.unusedID.size()-1)
                {
                    int pageID = Page.pageCount++;
                    unusedListBuffer.putInt(Page.unusedID.get(i));
                    unusedListBuffer.putInt(pageID);
                    fc.write(unusedListBuffer, pageID*Page.PAGE_SIZE+CacheMgr.FILEHEADERSIZE+CacheMgr.UNUSEDLISTSIZE);
                }
                else
                    unusedListBuffer.putInt(Page.unusedID.get(i));
            }

        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**开始一个新的事务，返回新事务的transID
     *
     * 生成一个新的事务对象，获得对应的锁
     * 记录到transMgr
     */
    public int beginTransation(String s)
    {
        Transaction trans = new Transaction(s, lock);
        trans.begin();
        this.transMgr.put(trans.transID, trans);
        return trans.transID;
    }

    /**提交transID对应的事务，将更新的副本页写回到cache中
	 * 1.先得到该事物修改过的page List，对于page list中的每个page 
     * 2.将cache中的对应的原始page（如果cache命中）或者文件中的page读出来保存到日志文件中
     * 3.如果cache命中，更新cache
     * 4.直接写回到文件中
     * 5.此时，若缓存已满，按照LRU策略将cacheList的第一页写入文件中
     */
    public boolean commitTransation(int transID) throws IOException {
        Transaction trans = transMgr.get(transID);
        if(trans.WR)
        {
            List<Page> writePageList= transOnPage.get(transID);
            File journal_file = new File(Integer.toString(transID)+"-journal");
            File db_file = new File(this.dbName);
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(journal_file));

            RandomAccessFile fin = new RandomAccessFile(journal_file, "rw");
            FileChannel fc = fin.getChannel();
            ByteBuffer IDBuffer = ByteBuffer.allocate(Page.PAGE_SIZE+4);
            if(writePageList != null)
            {
                for( int i = 0 ; i < writePageList.size() ; i++)
                {
                    Page copyPage = writePageList.get(i);
                    Page tempPage = this.cachePageMap.get(copyPage.pageID);
                    //cache未命中
                    if (tempPage == null) {
                        //cache已满，按照LRU策略替换
                        if (this.cachePageMap.size() >= CacheMgr.CacheCapacity) {
                            //按照LRU在cache的list和map中删除某一页的记录
                            tempPage = this.cacheList.get(0);
                            this.cacheList.remove(0);
                            this.cachePageMap.remove(tempPage.pageID);

                            //从文件中读入该页，新加入cache的list和map中
                            tempPage = getPageFromFile(copyPage.pageID);
                        }
                        //cache还有空间
                        else {
                            //从文件中读入该页，新加入cache的list和map中
                            tempPage = getPageFromFile(copyPage.pageID);
                        }
                    }
                    //cache命中，按照LRU更新cache的list
                    else {
                        for (int j = 0; j < cacheList.size(); j++) {
                            Page jPage = cacheList.get(j);
                            if (jPage.pageID == copyPage.pageID) {
                                cacheList.remove(j);
                            }
                        }
                    }

                    //此时tempPage都是文件中的原始数据，写日志文件
                    if (journal_file.exists() && journal_file.isFile()) {
                        //out.writeObject(tempPage);
                        IDBuffer.rewind();
                        IDBuffer.putInt(tempPage.pageID);
                        IDBuffer.put(tempPage.pageBuffer);
                        fc.write(IDBuffer);
                    }

                    byte [] copy_content = new byte[copyPage.getPageBuffer().limit()];
                    copyPage.getPageBuffer().rewind();
                    copyPage.getPageBuffer().get(copy_content);
                    System.out.println(copy_content);
                    //写cache
                    tempPage.pageID = copyPage.pageID;
                    tempPage.getPageBuffer().rewind();
                    tempPage.pageBuffer.put(copy_content, 0, copy_content.length);

                    byte [] temp_content = new byte[tempPage.getPageBuffer().limit()];
                    tempPage.getPageBuffer().rewind();
                    tempPage.getPageBuffer().get(temp_content);
                    System.out.println(temp_content);



                    this.cacheList.add(tempPage);
                    this.cachePageMap.put(tempPage.pageID, tempPage);
                    //写直达，将该页同时写至数据库文件中
                    if (db_file.exists() && db_file.isFile()) {
                        this.setPageToFile(tempPage, db_file);
                    }
                }

            }
        }
        this.transMgr.remove(transID);
        this.transOnPage.remove(transID);
        trans.commit();

        return true;
    }

    /**transID对应的事务回滚
     *
     * 释放对应的锁，cache不做任何操作
     */

    public boolean rollbackTransation(int transID)  {
        Transaction trans = transMgr.get(transID);


        if(trans.WR)
        {
            File journal_file = new File(Integer.toString(transID)+"-journal");
            File db_file = new File(this.dbName);
            ObjectInputStream in = null;
            try {
                in = new ObjectInputStream(new FileInputStream(journal_file));
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            try
            {
                while(in.available() > 1)
                {
                    Page srcPage = (Page)in.readObject();
                    Page tempPage = this.cachePageMap.get(srcPage.pageID);
                    //cache命中, 更改cache文件
                    if (tempPage != null)
                    {
                        tempPage.pageBuffer.put(srcPage.pageBuffer);
                    }
                    //将该页同时写至数据库文件中
                    //此处修改了一个bug by sqlu：不管是否cache命中，都要写回数据库文件
                    if (db_file.exists() && db_file.isFile()) {
                        this.setPageToFile(srcPage, db_file);
                    }
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            finally
            {
                if(in != null) try {
                    in.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        trans.rollback();
        this.transMgr.remove(transID);
        return true;
    }


    /**事务transID读取pageID对应的页，返回页的副本
     * 
     * 如果当前页已经被缓存入cachePageMap，则命中页，直接从缓存区中返回副本
     * 否则，页缺失，从文件中读取对应页到缓存区后返回副本
     * 此时，若缓存空间已满，按照LRU策略将一页写回文件，腾出空间后从文件中读取页
     * 在这个过程中，按照LRU策略更新cacheList
     */
    public Page readPage(int transID, int pageID)
    {

        Page tempPage = null;
        tempPage = this.cachePageMap.get(pageID);

        List<Page> writePageList= transOnPage.get(transID);
        if(writePageList != null)
        {
            for( int i = 0 ; i < writePageList.size() ; i++)
            {
                Page copyPage = writePageList.get(i);
                if(copyPage.pageID == pageID)
                    return copyPage;
            }
        }

        //cache未命中
        if(tempPage == null)
        {
            //cache已满，按照LRU策略替换，无需写回文件，删除cache中list和map记录即可
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
        //cache命中
        else
        {
            //按照LRU策略更新cacheList链表
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

    /**事务transID写pageID对应的页，只做记录，并未提交到Cache
     *
     *根据事务的transID获得写队列，将副本的引用添加到写列表中
     */
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

    /**事务transID删除pageID对应的页
     *
     *记录被删除页的ID
     */
    public void deletePage(int transID, int pageID)
    {
        Page.unusedID.add(pageID);
    }


    /**将指定的某一页写回至内存和文件
     *
     */
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
            //独占锁
            FileLock lock = fc.lock();
            tempPage.pageBuffer.flip();
            fc.write(tempPage.pageBuffer, tempPage.pageID*Page.PAGE_SIZE+CacheMgr.FILEHEADERSIZE+CacheMgr.UNUSEDLISTSIZE);
            lock.release();

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


    /**读取文件中的指定PageID页
     *
     */
    private Page getPageFromFile(int pageID)
    {
        Page tempPage = null;
        FileChannel fc = null;
        try
        {
            File file = new File(this.dbName);
            RandomAccessFile fin = new RandomAccessFile(file, "rw");
            fc = fin.getChannel();

            //共享锁
            FileLock lock = fc.lock(0, Long.MAX_VALUE, true);

            ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
            fc.read(tempBuffer, pageID*Page.PAGE_SIZE+CacheMgr.FILEHEADERSIZE+CacheMgr.UNUSEDLISTSIZE);
            tempPage = new Page(pageID, tempBuffer);

            lock.release();
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
