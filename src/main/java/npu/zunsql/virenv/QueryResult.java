package npu.zunsql.virenv;

import java.util.ArrayList;
import java.util.List;

public class QueryResult
{
    private List<List<String>> res = new ArrayList<>();
    private List<Column> header;
    private int affectedCount;

    public QueryResult(List<Column> pCol) {
        header=new ArrayList<Column>(pCol);
        affectedCount=0;
        return;
    }

    public QueryResult(){
        affectedCount=0;
    }

    public int getAffectedCount(){
        return  affectedCount;
    }

    public void setAffectedCount(int cnt){
        affectedCount=cnt;
    }
    public void addAffectedCount()
    {
        affectedCount++;
    }

    public boolean addRecord(List<String> pRecord)
    {
        return res.add(pRecord);
    }

    public List<Column> getHeader() {
        return header;
    }

    public List<String> getHeaderString() {
        List<String> result = new ArrayList<>();
        header.forEach(n -> result.add(n.ColumnName));
        return result;
    }

    public List<List<String>> getRes() {
        return res;
    }

//    public void setActivity(Activity type){
//        this.type = type;
//    }
//
//    private void constructSelect(){
//
//    }
//
//    private void constructUpdate(){
//
//    }
//
//    private void constructInsert(){
//
//    }
//
//    private void constructDelete(){
//
//    }
//
//    private void constructDrop(){
//
//    }
//
//    private void constructCreate(){
//
//    }
//
//    private void constructResult(){
//        switch (type){
//            case Select:
//                constructSelect();
//                break;
//            case Delete:
//                constructDelete();
//                break;
//            case Insert:
//                constructInsert();
//                break;
//            case Update:
//                constructUpdate();
//                break;
//            case CreateTable:
//                constructCreate();
//                break;
//            case DropTable:
//                constructDrop();
//                break;
//        }
//    }
//
//    private String getFinalResult(){
//        constructResult();
//        return result;
//    }

}