package npu.zunsql.treemng;

import npu.zunsql.cache.Page;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ed on 2017/10/28.
 */
public abstract class Cursor
{
    protected Cursor()
    {
        ;
    }

    // 获取列类型
    // 输入参数：columnName，列名。
    public abstract BasicType getColumnType(String columnName);

    // 获取某一列的单元字符串。
    // 输入参数：columnName，列名。
    public abstract String getCell_s(String columnName);

    // 获取某一列的单元整形。
    // 输入参数：columnName，列名。
    public abstract Integer getCell_i(String columnName);

    // 获取某一列的单元双精度。
    // 输入参数：columnName，列名。
    public abstract Double getCell_d(String columnName);

    // 获取主键单元字符串。
    public abstract String getKeyCell_s();

    // 获取主键单元整形。
    public abstract Integer getKeyCell_i();

    // 获取主键单元双精度。
    public abstract Double getKeyCell_d();

    // 游标移至首条
    public abstract boolean moveToFirst(Transaction thisTran) throws IOException, ClassNotFoundException;

    // 游标移至末尾
    public abstract boolean moveToLast(Transaction thisTran) throws IOException, ClassNotFoundException;

    // 游标后移一条
    public abstract boolean moveToNext(Transaction thisTran) throws IOException, ClassNotFoundException;

    // 游标前移一条
    public abstract boolean moveToPrevious(Transaction thisTran) throws IOException, ClassNotFoundException;

    // 游标移至指定位
    // 输入参数：key主键的字符串值
    public abstract boolean moveToUnpacked(Transaction thisTran,String key) throws IOException, ClassNotFoundException;

    // 游标移至指定位
    // 输入参数：key主键的整型值
    public abstract boolean moveToUnpacked(Transaction thisTran,Integer key) throws IOException, ClassNotFoundException;

    // 游标移至指定位
    // 输入参数：key主键的双精度值
    public abstract boolean moveToUnpacked(Transaction thisTran,Double key) throws IOException, ClassNotFoundException;

    // 删除本条
    public abstract boolean delete(Transaction thistran) throws IOException, ClassNotFoundException;

    // 插入一条
    public abstract boolean insert(Transaction thisTran,List<String> stringList) throws IOException, ClassNotFoundException;

    // 获取本条内容，字符串值
    public abstract List<String> getData();

    // 调整本条内容
    public abstract boolean setData(Transaction thisTran,List<String> stringList) throws IOException, ClassNotFoundException;
}


class TableCursor extends Cursor
{
    protected Table aimTable;
    protected int thisRowID;
    protected Node thisNode;

    protected TableCursor(Table thisTable, Transaction thisTran) throws IOException, ClassNotFoundException {
        super();
        aimTable = thisTable;
        thisRowID = 0;
        if(aimTable.rootNodePage == -1)
        {
            thisNode = null;
        }
        else
        {
            thisNode=aimTable.getRootNode(thisTran);
        }


    }

    public BasicType getColumnType(String columnName)
    {
        return aimTable.getColumn(columnName).getType();
    }

    // 获取某一列的单元字符串。
    // 输入参数：columnName，列名。
    public String getCell_s(String columnName)
    {
        return thisNode.getRow(thisRowID).getCell(aimTable.getColumn(columnName).getNumber()).getValue_s();
    }

    // 获取某一列的单元整形。
    // 输入参数：columnName，列名。
    public Integer getCell_i(String columnName)
    {
        if(aimTable.getColumn(columnName).getNumber() == 2)
        {
            int a = 0;
        }
        return thisNode.getRow(thisRowID).getCell(aimTable.getColumn(columnName).getNumber()-1).getValue_i();
    }

    // 获取某一列的单元双精度。
    // 输入参数：columnName，列名。
    public Double getCell_d(String columnName)
    {
        return thisNode.getRow(thisRowID).getCell(aimTable.getColumn(columnName).getNumber()).getValue_d();
    }

    // 获取主键单元字符串。
    public String getKeyCell_s()
    {
        return getCell_s(aimTable.getKeyColumn().getName());
    }

    // 获取主键单元整形。
    public Integer getKeyCell_i()
    {
        return getCell_i(aimTable.getKeyColumn().getName());
    }

    // 获取主键单元双精度。
    public Double getKeyCell_d()
    {
        return getCell_d(aimTable.getKeyColumn().getName());
    }

    // 游标移至首条
    public boolean moveToFirst(Transaction thisTran) throws IOException, ClassNotFoundException {
        thisNode = aimTable.getRootNode(thisTran);
        while(thisNode.getSonNodeList()!=null)
        {
            thisNode = thisNode.getSpecialSonNode(0,thisTran);
        }
        thisRowID = 0;
        return true;
    }

    // 游标移至末尾
    public boolean moveToLast(Transaction thisTran) throws IOException, ClassNotFoundException {
        //thisNode = aimTable.getRootNode(thisTran);
        while(thisNode.getSonNodeList().size() != 0)
        {
            thisNode = thisNode.getSpecialSonNode(thisNode.getSonNodeList().size()-1,thisTran);
        }

        return true;
    }

    // 游标后移一条
    public boolean moveToNext(Transaction thisTran) throws IOException, ClassNotFoundException {
        //int flagchang=0;
        if(thisRowID<thisNode.getRowList().size()-1)
        {
            thisRowID++;
            return true;
        }
        else
        {
            while(thisNode.getFatherNodeID()>0)
            {
                if(thisNode.getOrder()<thisNode.getFatherNode(thisTran).getSonNodeList().size()-1)
                {
                    thisNode=thisNode.getFatherNode(thisTran).getSpecialSonNode(thisNode.getOrder()+1,thisTran);
                    while(thisNode.getSonNodeList()!=null)
                    {
                        thisNode = thisNode.getSpecialSonNode(0,thisTran);
                    }
                    thisRowID=0;
                    return true;
                }
                else    //如果当前结点的父亲结点位是当前结点的祖父结点的最后一个儿子结点
                {
                        thisNode=thisNode.getFatherNode(thisTran);

                }
            }
        }
        moveToLast(thisTran);
        return false;
    }

    // 游标前移一条
    public boolean moveToPrevious(Transaction thisTran) throws IOException, ClassNotFoundException {
        if(thisRowID>0)
        {
            thisRowID--;
            return true;
        }
        else
        {
            while(thisNode.getFatherNodeID()>0)
            {
                if(thisNode.getOrder()>0)
                {
                    thisNode=thisNode.getFatherNode(thisTran).getSpecialSonNode(thisNode.getOrder()-1,thisTran);
                    while(thisNode.getSonNodeList()!=null)
                    {
                        thisNode = thisNode.getSpecialSonNode(thisNode.getSonNodeList().size()-1,thisTran);
                    }
                    thisRowID=thisNode.getRowList().size()-1;
                    return true;
                }
                else    //如果当前结点的父亲结点位是当前结点的祖父结点的第一个儿子结点
                {
                    thisNode=thisNode.getFatherNode(thisTran);

                }
            }
        }
        moveToFirst(thisTran);
        return false;
    }

    private boolean moveToSon(Transaction thisTran,Cell key) throws IOException, ClassNotFoundException {
        if(thisNode.getSonNodeList().size() == 0)
        {
            return false;
        }
        for(int i = 0; i < thisNode.getRowList().size(); i++)
        {
            if(key.equalTo(thisNode.getRowList().get(i).getCell(0)))
            {
                thisRowID = i;
                return true;
            }
        }
        for (int i = 0; i < thisNode.getRowList().size(); i++)
        {
            if(thisNode.getRowList().get(i).getCell(0).bigerThan(key))
            {
                thisNode = thisNode.getSpecialSonNode(i,thisTran);
                return moveToSon(thisTran,key);
            }
        }
        if(key.bigerThan(thisNode.getRowList().get(thisNode.getRowList().size()-1).getCell(0)))
        {
            thisNode = thisNode.getSpecialSonNode(thisNode.getRowList().size()-1,thisTran);
            return moveToSon(thisTran,key);
        }
        return false;
    }

    // 游标移至指定位
    // 输入参数：key主键的字符串值
    public boolean moveToUnpacked(Transaction thisTran,String key) throws IOException, ClassNotFoundException
    {
        if(aimTable.rootNodePage == -1)
        {
            return false;
        }
        thisNode = aimTable.getRootNode(thisTran);
        thisRowID = 0;
        moveToSon(thisTran,new Cell(key));
        return true;
    }

    // 游标移至指定位
    // 输入参数：key主键的整型值
    public boolean moveToUnpacked(Transaction thisTran,Integer key) throws IOException, ClassNotFoundException {
        thisNode = aimTable.getRootNode(thisTran);
        thisRowID = 0;
        moveToSon(thisTran,new Cell(key.toString()));
        return true;
    }

    // 游标移至指定位
    // 输入参数：key主键的双精度值
    public boolean moveToUnpacked(Transaction thisTran,Double key) throws IOException, ClassNotFoundException {
        thisNode = aimTable.getRootNode(thisTran);
        thisRowID = 0;
        moveToSon(thisTran,new Cell(key.toString()));
        return true;
    }

    // 删除本条
    public boolean delete(Transaction thisTran) throws IOException, ClassNotFoundException {
        Cell keyCell = thisNode.getRow(thisRowID).getCell(0);
        moveToNext(thisTran);
        Cell nextCell = thisNode.getRow(thisRowID).getCell(0);
        aimTable.getRootNode(thisTran).deleteRow(keyCell,thisTran);
        moveToUnpacked(thisTran,nextCell.getValue_s());
        return true;
    }

    // 插入一条
    public boolean insert(Transaction thisTran,List<String> stringList) throws IOException, ClassNotFoundException {
        Row row = new Row(stringList);
        if(aimTable.rootNodePage == -1)
        {
            ByteBuffer tempBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
            byte [] bytes=new byte[Page.PAGE_SIZE];
            ByteArrayOutputStream byt=new ByteArrayOutputStream();
            ObjectOutputStream obj=new ObjectOutputStream(byt);
            List<Node> sonList = new ArrayList<Node>();
            obj.writeObject(sonList);
            obj.writeObject(-1);
            obj.writeObject(-1);
            List<Row> rowList = new ArrayList<Row>();
            rowList.add(row);
            obj.writeObject(rowList);
            bytes=byt.toByteArray();
            tempBuffer.put(bytes);
            Page rootPage = new Page(tempBuffer);
            aimTable.cacheManager.writePage(thisTran.tranNum,rootPage);
            aimTable.writeRootNodePage(rootPage.getPageID(),thisTran);
            Node rootNode = new Node(rootPage.getPageID(),aimTable.cacheManager,thisTran);
            int a = 0;
            //rootNode.insertRow(row,thisTran);
        }
        else
        {
            aimTable.getRootNode(thisTran).insertRow(row, thisTran);
        }
        return moveToUnpacked(thisTran,row.getCell(0).getValue_s());
    }

    // 获取本条内容，字符串值
    public List<String> getData()
    {
        return thisNode.getRow(thisRowID).getStringList();
    }

    // 调整本条内容
    public boolean setData(Transaction thistran,List<String> stringList) throws IOException, ClassNotFoundException {
        if(!delete(thistran))
        {
            return false;
        };
        if(!insert(thistran,stringList))
        {
            return false;
        };
        return true;
    }
}

class ViewCursor extends Cursor
{
    protected View aimView;
    protected int RowID;

    protected ViewCursor(View aView)
    {
        super();
        aimView = aView;
        RowID = 0;
    }

    // 获取列类型
    // 输入参数：columnName，列名。
    public BasicType getColumnType(String columnName)
    {
        return aimView.getColumn(columnName).getType();
    }

    // 获取某一列的单元字符串。
    // 输入参数：columnName，列名。
    public String getCell_s(String columnName)
    {
        return aimView.rowList.get(RowID).getCell(aimView.getColumn(columnName).getNumber()).getValue_s();
    }

    // 获取某一列的单元整形。
    // 输入参数：columnName，列名。
    public Integer getCell_i(String columnName)
    {
        return aimView.rowList.get(RowID).getCell(aimView.getColumn(columnName).getNumber()).getValue_i();
    }

    // 获取某一列的单元双精度。
    // 输入参数：columnName，列名。
    public Double getCell_d(String columnName)
    {
        return aimView.rowList.get(RowID).getCell(aimView.getColumn(columnName).getNumber()).getValue_d();
    }

    // 获取第一列单元字符串。
    public String getKeyCell_s()
    {
        return aimView.rowList.get(RowID).getCell(0).getValue_s();
    }

    // 获取第一列单元整形。
    public Integer getKeyCell_i()
    {
        return aimView.rowList.get(RowID).getCell(0).getValue_i();
    }

    // 获取第一列单元双精度。
    public Double getKeyCell_d()
    {
        return aimView.rowList.get(RowID).getCell(0).getValue_d();
    }

    // 游标移至首条
    public boolean moveToFirst(Transaction thisTran)
    {
        RowID = 0;
        return true;
    }

    // 游标移至末尾
    public boolean moveToLast(Transaction thisTran)
    {
        RowID = aimView.rowList.size() - 1;
        return true;
    }

    // 游标后移一条
    public boolean moveToNext(Transaction thisTran)
    {
        if(RowID < aimView.rowList.size() - 1)
        {
            RowID++;
            return true;
        }
        else
        {
            return false;
        }
    }

    // 游标前移一条
    public boolean moveToPrevious(Transaction thisTran)
    {
        if(RowID > 0)
        {
            RowID--;
            return true;
        }
        else
        {
            return false;
        }
    }

    // 游标移至指定位
    // 输入参数：key主键的字符串值
    public boolean moveToUnpacked(Transaction thisTran,String key)
    {
        return false;
    }

    // 游标移至指定位
    // 输入参数：key主键的整型值
    public boolean moveToUnpacked(Transaction thisTran,Integer key)
    {
        return false;
    }

    // 游标移至指定位
    // 输入参数：key主键的双精度值
    public boolean moveToUnpacked(Transaction thisTran,Double key)
    {
        return false;
    }

    // 删除本条
    public boolean delete(Transaction thistran)
    {
        return false;
    }

    // 插入一条
    public boolean insert(Transaction thisTran,List<String> stringList)
    {
        return false;
    }

    // 获取本条内容，字符串值
    public List<String> getData()
    {
        return aimView.rowList.get(RowID).getStringList();
    }

    // 调整本条内容
    public boolean setData(Transaction thisTran,List<String> stringList)
    {
        if(!delete(thisTran))
        {
            return false;
        }
        if(!insert(thisTran,stringList))
        {
            return false;
        }
        return true;
    }
}