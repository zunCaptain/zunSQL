package npu.zunsql.cache;

public class FileHeader
{
    protected String version;
    public static String magicNum;
    public static final int fileHeaderSize = 1024;

    FileHeader(String version, String magicNum)
    {
        this.version = version;
        this.magicNum = magicNum;
    }
    boolean isValid()
    {
        if(this.version == "0.1" && this.magicNum == "3.1415926")
            return true;
        else
            return false;
    }
}
