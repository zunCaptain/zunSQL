package npu.zunsql.ve;

import npu.zunsql.tree.BasicType;

public class UnionOperand {
    private String value;
    private BasicType type;
    public UnionOperand(BasicType pType,String pValue){
        value=pValue;
        type=pType;
        if(type == BasicType.Integer){
            value = (int)(double)Double.valueOf(value)+"";
        }
    }
    public BasicType getType() {
        return type;
    }
    public String getValue(){
        return value;
    }
}
