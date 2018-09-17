package npu.zunsql.tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ed on 2017/10/30.
 */
public class Row implements Serializable
{
    // 每个Row中包含一个所有列
    protected List<Cell> cellList = new ArrayList<Cell>();

    protected Row(List<String> SList)
    {
        for(int i = 0; i < SList.size(); i++)
        {
            cellList.add(new Cell(SList.get(i)));
        }
    }

    protected List<String> getStringList()
    {
        List<String> SList = new ArrayList<String>();
        for(int i = 0; i < cellList.size(); i++)
        {
            SList.add(cellList.get(i).getValue_s());
        }
        return SList;
    }

    protected Cell getCell(int array)
    {

        return cellList.get(array);
}
}
