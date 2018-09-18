package npu.zunsql.treemng;

import java.io.IOException;
import java.util.List;

/**
 * Created by Ed on 2017/10/28.
 */
public interface TableReader
{
    public abstract List<String> getColumnsName();

    public abstract List<BasicType> getColumnsType();

    public abstract Cursor createCursor(Transaction thistran) throws IOException, ClassNotFoundException;
}


