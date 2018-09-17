package npu.zunsql.tree;

import npu.zunsql.cache.CacheMgr;

import java.io.IOException;

/**
 * Created by Ed on 2017/10/28.
 */
public abstract class Transaction
{
    // transaction中包含一个事务号。
    protected Integer tranNum;

    protected CacheMgr cacheMagr;
    // transaction初始化一个事务号
    protected Transaction(Integer number,CacheMgr thisCacheMgr)
    {
        tranNum = number;
        cacheMagr = thisCacheMgr;
    }

    // 定义了两个抽象操作，提交和回滚。
    public abstract boolean Commit() throws IOException;

    public abstract boolean RollBack() throws IOException;
}

class WriteTran extends Transaction
{
    protected WriteTran(int num,CacheMgr thisCacheMgr)
    {
        super(num,thisCacheMgr);
    }

    public boolean Commit() throws IOException {
        System.out.print(tranNum);
        cacheMagr.commitTransation(tranNum);
        return true;
    }

    public boolean RollBack() throws IOException {
        return cacheMagr.rollbackTransation(tranNum);
    }
}

class ReadTran extends Transaction
{
    protected ReadTran(int num,CacheMgr thisCacheMgr)
    {
        super(num,thisCacheMgr);
    }

    public boolean Commit()
    {
        try {
            cacheMagr.commitTransation(tranNum);
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public boolean RollBack() throws IOException {
        return cacheMagr.rollbackTransation(tranNum);
    }
}