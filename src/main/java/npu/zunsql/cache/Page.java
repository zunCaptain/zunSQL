package npu.zunsql.cache;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Page implements Serializable
{
    public static final int PAGE_SIZE = 102400;

    protected static int pageCount = 0;
    protected static List<Integer> unusedID =  new ArrayList<Integer>();

    protected int pageID;
    protected ByteBuffer pageBuffer = null;

    //
    public Page(ByteBuffer buffer)
    {
        //TODO:unusedLIst
        if(this.unusedID.isEmpty())
            this.pageID = pageCount++;
        else
        {
            this.pageID = this.unusedID.indexOf(0);
            this.unusedID.remove(0);
        }
        this.pageBuffer = buffer;
    }

    public Page(int pageID, ByteBuffer buffer)
    {
        this.pageID = pageID;
        this.pageBuffer = buffer;
    }

    //拷贝构造方法
    public Page(Page page)
    {
        this.pageID = page.pageID;
        ByteBuffer tempBuffer = ByteBuffer.allocate(page.pageBuffer.capacity());
        tempBuffer.put(page.pageBuffer);
        this.pageBuffer = tempBuffer;
    }

    public int getPageID()
    {
        return this.pageID;
    }

    public ByteBuffer getPageBuffer()
    {
        return this.pageBuffer;
    }
}
