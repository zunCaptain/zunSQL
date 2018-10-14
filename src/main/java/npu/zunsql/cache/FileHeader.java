package npu.zunsql.cache;

public class FileHeader
{
    protected int version;
    public static int magicNum;
    public static final int fileHeaderSize = 1024;

    FileHeader(int version, int magicNum)
    {
        this.version = version;
        FileHeader.magicNum = magicNum;
    }
    boolean isValid()
    {
        if(this.version == 1 && FileHeader.magicNum == 123)
            return true;
        else
            return false;
    }
}
