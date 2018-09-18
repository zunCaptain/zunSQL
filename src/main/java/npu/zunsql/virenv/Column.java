package npu.zunsql.virenv;


import npu.zunsql.treemng.BasicType;

class Column
{
    String ColumnName;
    String ColumnType;

    public Column(String pName,String pType)
    {
        ColumnName=pName;
        ColumnType=pType;
    }

    public Column(String pName)
    {
        ColumnName=pName;
    }

    @Override
    public boolean equals(Object obj) {
        Column other = (Column) obj;
        if(this.ColumnName.equals(other.ColumnName))
            return true;
        else return false;
    }

    public String getColumnName() {
        return ColumnName;
    }

    public String getColumnType() {
        return ColumnType;
    }

    public BasicType getColumnTypeBasic() {
        switch(ColumnType){
            case "String":
                return BasicType.String;
            case "Integer":
                return BasicType.Integer;
            case "Float":
                return BasicType.Float;
        }
        return null;
    }

}