package npu.zunsql.treemng;

import java.util.ArrayList;
import java.util.List;

public class View implements TableReader
{
    protected List<Column> columns;
    protected List<Row> rowList;
    protected View(List<String> columnNameList, List<BasicType> columnTypeList, List<List<String>> rowStringList)
    {
        super();
        columns = new ArrayList<Column>();
        rowList = new ArrayList<Row>();
        for(int i = 0; i < columnNameList.size(); i++)
        {
            columns.add(new Column(columnTypeList.get(i),columnNameList.get(i),i));
        }
        for(int i = 0; i < rowStringList.size(); i++)
        {
            rowList.add(new Row(rowStringList.get(i)));
        }
    }

    public List<String> getColumnsName()
    {
        List<String> sList = new ArrayList<String>();
        for(int i = 0; i < columns.size(); i++)
        {
            sList.add(columns.get(i).getName());
        }
        return sList;
    }

    public List<BasicType> getColumnsType()
    {
        List<BasicType> sList = new ArrayList<BasicType>();
        for(int i = 0; i < columns.size(); i++)
        {
            sList.add(columns.get(i).getType());
        }
        return sList;
    }

    public Cursor createCursor(Transaction thisTran)
    {
        return new ViewCursor(this);
    }

    protected Column getColumn(String columnName)
    {
        for(int i = 0; i < columns.size(); i++)
        {
            if(columns.get(i).getName().equals(columnName))
            {
                return columns.get(i);
            }
        }
        return null;
    }
}