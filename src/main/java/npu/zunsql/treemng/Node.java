package npu.zunsql.treemng;

import npu.zunsql.cache.CacheMgr;
import npu.zunsql.cache.Page;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
/**
 * Created by WQT on 2017/11/6.
 */

// 本Node类用于组织B树内部结构
// 每个Node包含一个Row列表和一个SonNode列表，其中SonNode列表除初始化时外，始终比Row列表多一个。
public class Node {

    // 需要存入page
    // 每个节点包含不少于M/2+1，不超过M+1的SonNode。
    private List<Integer> sonNodeList;

    // 需要存入page
    // 表示父亲节点
    private int fatherNodeID;

    // 需要存入page
    // 表示本节点在父亲节点儿子中的第几位。
    private int order;

    // 需要存入page
    // 每个节点包含不少于M/2，不超过M的Row。
    private List<Row> rowList;

    // 用于表示本树为几阶B树。
    public static int M = 3;

    // 用于操作存储page
    private CacheMgr cacheManager;

    // 每个Node表示一个Page
    private Page pageOne;

    //加载
    protected Node(int thisPageID, CacheMgr cacheManager, Transaction thisTran) throws IOException, ClassNotFoundException {
        this.cacheManager = cacheManager;
        pageOne = this.cacheManager.readPage(thisTran.tranNum, thisPageID);
        // 根据thisPage加载本Node信息

        ByteBuffer thisBufer = pageOne.getPageBuffer();
        byte [] bytes=new byte[Page.PAGE_SIZE] ;
        thisBufer.rewind();
        thisBufer.get(bytes,0,thisBufer.remaining());

        ByteArrayInputStream byteTable=new ByteArrayInputStream(bytes);
        ObjectInputStream objTable=new ObjectInputStream(byteTable);

        this.sonNodeList=(List<Integer>)objTable.readObject();
        this.fatherNodeID=(int) objTable.readObject();
        this.order=(int)objTable.readObject();
        this.rowList=(List<Row>)objTable.readObject();

    }

    // 根据Node的属性构造Node。
    private Node(List<Row> thisRowList, List<Integer> thisSonList, int thisOrder, CacheMgr cacheManager, Transaction thisTran) throws IOException, ClassNotFoundException {
        ByteBuffer buffer = ByteBuffer.allocate(Page.PAGE_SIZE);
        rowList = thisRowList;
        sonNodeList = thisSonList;
        fatherNodeID = -1;
        this.cacheManager = cacheManager;
        pageOne = new Page(buffer);
        // 为每一位儿子维护父亲和排位信息。
        for (int i = 0; i < sonNodeList.size(); i++)
        {
            Node sonNode = new Node(sonNodeList.get(i),cacheManager,thisTran);
            sonNode.setFather(pageOne.getPageID(),thisTran);
            sonNode.setOrder(i,thisTran);
        }

        // 维护自身排位信息。
        order = thisOrder;
        // 序列化信息至buffer
        intoBytes(thisTran);

    }

    protected void intoBytes (Transaction thisTran) throws IOException {
        byte [] bytes=new byte[Page.PAGE_SIZE] ;
        ByteArrayOutputStream byt=new ByteArrayOutputStream();
        ObjectOutputStream obj=new ObjectOutputStream(byt);
        obj.writeObject(sonNodeList);
        obj.writeObject(fatherNodeID);
        obj.writeObject(order);
        obj.writeObject(rowList);
        bytes=byt.toByteArray();
        pageOne.getPageBuffer().rewind();
        pageOne.getPageBuffer().put(bytes);
        cacheManager.writePage(thisTran.tranNum,pageOne);
        //thisTran.Commit();
    }



    private boolean setFather(int ID,Transaction thisTran) throws IOException {
        fatherNodeID = ID;

        // 维护page信息
        intoBytes(thisTran);

        return true;
    }


    private boolean setOrder(int or, Transaction thisTran) throws IOException {
        order = or;

        // 维护page信息
        intoBytes(thisTran);

        return true;
    }

    // 分裂除根节点外的其他节点。
    private Node devideNode(Transaction thisTran) throws IOException, ClassNotFoundException {
        List<Row> rightRow;
        List<Integer> rightNode;
        rightRow = subRowList(rowList,M/2 + 1, M-1);
        rowList = subRowList(rowList,0, M/2);
        if(sonNodeList.size() == 0)
        {
            rightNode = new ArrayList<Integer>();
        }
        else
        {
            rightNode = subNodeList(sonNodeList,M/2 + 1,M);
            sonNodeList = subNodeList(sonNodeList,0, M/2 + 1);
        }

        //维护本page信息
        intoBytes(thisTran);

        return new Node(rightRow, rightNode, order + 1, cacheManager, thisTran);
    }

    // 分裂根节点
    private boolean rootDevideNode(Transaction thisTran) throws IOException, ClassNotFoundException {
        List<Row> leftRow;
        List<Row> rightRow;
        List<Integer> leftNode;
        List<Integer> rightNode;
        leftRow = subRowList(rowList,0,M/2-1);
        if(sonNodeList.size() == 0)
        {
            leftNode = new ArrayList<Integer>();
        }
        else
        {
            leftNode = subNodeList(sonNodeList,0,M/2);
        }
        rightRow = subRowList(rowList,M/2 + 1, M-1);
        if(sonNodeList.size() == 0)
        {
            rightNode = new ArrayList<Integer>();
        }
        else
        {
            rightNode =subNodeList(sonNodeList,M/2 + 1, M);
        }
        rowList = subRowList(rowList,M/2, M/2);
        List<Integer> newSonList = new ArrayList<Integer>();
        newSonList.add(new Node(leftRow, leftNode, 0, cacheManager, thisTran).pageOne.getPageID());
        newSonList.add(new Node(rightRow, rightNode,1, cacheManager, thisTran).pageOne.getPageID());
        sonNodeList = newSonList;

        //维护本page信息
        intoBytes(thisTran);

        return true;
    }

    private List<Integer> subNodeList(List<Integer>list, int a, int b)
    {
        List<Integer> ret = new ArrayList<Integer>();
        for(int i = a; i <= b; i++)
        {
            ret.add(list.get(i));
        }
        return ret;
    }

    private List<Row> subRowList(List<Row>list, int a, int b)
    {
        List<Row> ret = new ArrayList<Row>();
        for(int i = a; i <= b; i++)
        {
            ret.add(list.get(i));
        }
        return ret;
    }

    // 调整本节点使其顺序为sonOrder的儿子row数量恢复至M/2
    private boolean adjustNode(int sonOrder, Transaction thisTran) throws IOException, ClassNotFoundException {
        Node thisSonNode = new Node(sonNodeList.get(sonOrder),cacheManager,thisTran);

        // 排除最大值边界越界情况，向左下合并
        if (sonOrder < sonNodeList.size() - 1)
        {
            Node rightSonNode = new Node(sonNodeList.get(sonOrder + 1), cacheManager, thisTran);
            if (rightSonNode.rowList.size() > M/2)
            {
                thisSonNode.insertRow(rowList.get(sonOrder),thisTran);
                rightSonNode.deleteRow(rowList.get(order).getCell(0),thisTran);
                rowList.set(sonOrder, rightSonNode.getFirstRow(thisTran));
                //维护本page信息
                intoBytes(thisTran);
                return true;
            }
        }

        // 排除零值边界越界情况，向右下合并
        if (sonOrder > 0)
        {
            Node leftSonNode = new Node(sonNodeList.get(sonOrder - 1),cacheManager,thisTran);
            if (leftSonNode.rowList.size() > M/2)
            {
                thisSonNode.insertRow(rowList.get(sonOrder - 1),thisTran);
                leftSonNode.deleteRow(rowList.get(order).getCell(0),thisTran);
                rowList.set(sonOrder - 1, leftSonNode.getLastRow(thisTran));
                //维护本page信息
                intoBytes(thisTran);
                return true;
            }
        }

        // 没有相邻的可支援兄弟节点，只好删除此节点。
        return deleteNode(sonOrder,thisTran);
    }

    // 在本节点中添加子节点，分别添加row和对应的SonNode。
    private boolean addNode(Row row, Node node, Transaction thisTran) throws IOException, ClassNotFoundException {
        // 用于记录是否添加了这个节点。
        boolean addOrNot = false;
        for (int i = 0; i < rowList.size(); i++)
        {
            Row thisRow = rowList.get(i);
            Node thisNode = new Node(sonNodeList.get(i),cacheManager,thisTran);
            if (!addOrNot && thisRow.getCell(0).bigerThan(row.getCell(0)))
            {
                rowList.add(i, row);
                sonNodeList.add(i, node.pageOne.getPageID());
                // 维护page信息。
                intoBytes(thisTran);
                thisNode.setFather(pageOne.getPageID(),thisTran);
                addOrNot = true;
            }
            thisNode.setOrder(i,thisTran);
        }
        // 如果之前都没有添加这个节点，那么此时添加至末尾。
        if (!addOrNot)
        {
            rowList.add(row);
            sonNodeList.add(sonNodeList.size() - 2, node.pageOne.getPageID());
            // 维护page信息。
            intoBytes(thisTran);
            node.setOrder(sonNodeList.size() - 1,thisTran);
            node.setFather(pageOne.getPageID(),thisTran);
        }

        // 当未超出长度时，插入完毕。
        if (rowList.size() <= M)
        {
            return true;
        }
        // 超出长度时，进行单元分裂。
        else
        {
            if (fatherNodeID < 0)
            {
                return rootDevideNode(thisTran);
            }
            else
            {
                Node fatherNode = new Node(fatherNodeID,cacheManager,thisTran);
                Node nodeTwo = devideNode(thisTran);
                return fatherNode.addNode(rowList.get(M/2),nodeTwo,thisTran);
            }
        }
    }

    private boolean deleteNode(int sonOrder,Transaction thisTran) throws IOException, ClassNotFoundException {
        Row thisRow;
        if (sonOrder < sonNodeList.size() - 1)
        {
            thisRow = rowList.get(sonOrder);
            Node rightNode = new Node(sonNodeList.get(sonOrder + 1),cacheManager,thisTran);
            rightNode.insertRow(thisRow,thisTran);
            rowList.remove(sonOrder);
            sonNodeList.remove(sonOrder);
            // 维护page信息
            intoBytes(thisTran);
            for (int i = sonOrder; i < sonNodeList.size(); i++)
            {
                new Node(sonNodeList.get(i),cacheManager,thisTran).setOrder(i,thisTran);
            }
            if (rowList.size() < M/2)
            {
                if (fatherNodeID < 0)
                {
                    if (rowList.size() < 1)
                    {
                        rowList = new Node(sonNodeList.get(0),cacheManager,thisTran).rowList;
                        sonNodeList = new Node(sonNodeList.get(0),cacheManager,thisTran).sonNodeList;
                        // 维护page信息
                        intoBytes(thisTran);
                        return true;
                    }
                    else
                    {
                        return true;
                    }
                }
                else
                {
                    return new Node(fatherNodeID,cacheManager,thisTran).adjustNode(order,thisTran);
                }
            }
            else
            {
                return true;
            }
        }
        else
        {
            thisRow = rowList.get(sonOrder - 1);
            Node leftNode = new Node(sonNodeList.get(sonOrder - 1),cacheManager,thisTran);
            leftNode.insertRow(thisRow,thisTran);

            rowList.remove(sonOrder - 1);
            sonNodeList.remove(sonOrder);
            // 维护page信息
            intoBytes(thisTran);
            for (int i = sonOrder; i < sonNodeList.size(); i++)
            {
                new Node(sonNodeList.get(i),cacheManager,thisTran).setOrder(i,thisTran);
            }
            if (rowList.size() < M/2)
            {
                if (fatherNodeID < 0)
                {
                    if (rowList.size() < 1)
                    {
                        rowList = new Node(sonNodeList.get(0),cacheManager,thisTran).rowList;
                        sonNodeList = new Node(sonNodeList.get(0),cacheManager,thisTran).sonNodeList;
                        // 维护page信息
                        intoBytes(thisTran);
                        return true;
                    }
                    else
                    {
                        return true;
                    }
                }
                else
                {
                    return new Node(fatherNodeID,cacheManager,thisTran).adjustNode(order,thisTran);
                }
            }
            else
            {
                return true;
            }
        }
    }

    protected List<Integer> getSonNodeList()
    {
        return sonNodeList;

    }

    protected int getFatherNodeID()
    {
        return fatherNodeID;
    }

    protected Node getSpecialSonNode(int sonOrder,Transaction thisTran) throws IOException, ClassNotFoundException {
        return new Node(sonNodeList.get(sonOrder),cacheManager,thisTran);
    }

    protected List<Row> getRowList()
    {
        return rowList;
    }
    protected Node getFatherNode(Transaction thisTran) throws IOException, ClassNotFoundException
    {
        return new Node(fatherNodeID,cacheManager,thisTran);
    }
    protected int getOrder()
    {
        return order;
    }

    public boolean insertRow(Row row,Transaction thisTran) throws IOException, ClassNotFoundException {
        boolean insertOrNot = false;
        int insertNumber = 0;
        for (int i = 0; i < rowList.size(); i++)
        {
            if (rowList.get(i).getCell(0).equalTo(row.getCell(0)))
            {
                return false;
            }
            else if (rowList.get(i).getCell(0).bigerThan(row.getCell(0)))
            {
                insertNumber = i;
                insertOrNot = true;
                break;
            }
        }
        if (!insertOrNot)
        {
            insertNumber = rowList.size();
        }
        if ((sonNodeList == null)|| (sonNodeList.size() == 0))
        {
            rowList.add(insertNumber,row);
            // 维护page信息
            intoBytes(thisTran);
            if (rowList.size() <= M)
            {
                return true;
            }
            else
            {
                if (fatherNodeID < 0)
                {
                    return rootDevideNode(thisTran);
                }
                else
                {
                    return new Node(fatherNodeID,cacheManager,thisTran).addNode(rowList.get(M/2),devideNode(thisTran),thisTran);
                }
            }
        }
        else
        {
            return new Node(sonNodeList.get(insertNumber),cacheManager,thisTran).insertRow(row,thisTran);
        }
    }

    public boolean drop(Transaction thisTran)
    {
        // TODO:递归清理所有节点
        return true;
    }

    public boolean deleteRow(Cell key,Transaction thisTran) throws IOException, ClassNotFoundException {
        boolean deleteOrNot = false;
        int deleteNumber = 0;
        for (int i = 0; i < rowList.size(); i++)
        {
            Row thisRow = rowList.get(i);
            if (thisRow.getCell(0).equalTo(key))
            {
                if ((sonNodeList == null) || (sonNodeList.size() == 0))
                {
                    rowList.remove(i);
                    // 维护page信息
                    intoBytes(thisTran);
                    if (rowList.size() < M/2)
                    {
                        if (fatherNodeID < 0)
                        {
                        	if ((rowList.size() < 1) &&  (sonNodeList.size() < 1)) {
                        		return true;
                        	} else if (rowList.size() < 1)
                            {
                                rowList = new Node(sonNodeList.get(0),cacheManager,thisTran).rowList;
                                sonNodeList = new Node(sonNodeList.get(0),cacheManager,thisTran).sonNodeList;
                                // 维护page信息
                                intoBytes(thisTran);
                                return true;
                            }
                            else
                            {
                                return true;
                            }
                        }
                        else
                        {
                            return new Node(fatherNodeID,cacheManager,thisTran).adjustNode(order,thisTran);
                        }
                    }
                    else
                    {
                        return true;
                    }
                }
                else
                {
                    Row tempRow = getFirstRow(thisTran);
                    rowList.set(i, tempRow);
                    // 维护page信息
                    intoBytes(thisTran);
                    return new Node(sonNodeList.get(i + 1),cacheManager,thisTran).deleteRow(tempRow.getCell(0),thisTran);
                }
            }
            else if (thisRow.getCell(0).bigerThan(key))
            {
                deleteNumber = i;
                deleteOrNot = true;
                break;
            }
        }
        if (!deleteOrNot)
        {
            deleteNumber = rowList.size();
        }
        if ((sonNodeList == null) || (sonNodeList.size() == 0))
        {
            return false;
        }
        else
        {
            return new Node(sonNodeList.get(deleteNumber),cacheManager,thisTran).deleteRow(key,thisTran);
        }
    }

    public Row getRow(int id)
    {
        if (rowList.size() > 0)
        {
            return rowList.get(id);
        }
        else
        {
            return null;
        }
    }

    public Row getFirstRow(Transaction thisTran) throws IOException, ClassNotFoundException {
    	if((rowList == null) || (rowList.size() == 0)) {
    		return null;
    	}
    	
        if ((sonNodeList == null) || (sonNodeList.size() == 0))
        {
            return rowList.get(0);
        }
        else
        {
            return new Node(sonNodeList.get(0),cacheManager,thisTran).getFirstRow(thisTran);
        }
    }

    public Row getLastRow(Transaction thisTran) throws IOException, ClassNotFoundException {
        if ((sonNodeList == null) || (sonNodeList.size() == 0))
        {
            return rowList.get(rowList.size() - 1);
        }
        else
        {
            return new Node(sonNodeList.get(sonNodeList.size() - 1),cacheManager,thisTran).getLastRow(thisTran);
        }
    }

    public Row getSpecifyRow(Cell key,Transaction thisTran) throws IOException, ClassNotFoundException {
        int insertNumber = -1;
        for (int i = 0; i < rowList.size(); i++)
        {
            Row thisRow = rowList.get(i);
            if (thisRow.getCell(0).equalTo(key))
            {
                return thisRow;
            }
            else if (thisRow.getCell(0).bigerThan(key))
            {
                if ((sonNodeList == null) || (sonNodeList.size() == 0))
                {
                    insertNumber = i;
                    break;
                }
                else
                {
                    return new Node(sonNodeList.get(i),cacheManager,thisTran).getSpecifyRow(key,thisTran);
                }
            }
        }
        if ((sonNodeList == null) || (sonNodeList.size() == 0))
        {
            if (insertNumber > 0)
            {
                return rowList.get(insertNumber);
            }
            else
            {
                return null;
            }
        }
        else
        {
            return new Node(sonNodeList.get(sonNodeList.size() - 1),cacheManager,thisTran).getSpecifyRow(key,thisTran);
        }

    }



}
