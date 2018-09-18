package npu.zunsql.virenv;

public class EvalDiscription{
	OpCode cmd;
	String col_name;
	String constant;
	public EvalDiscription(OpCode pCmd,String pColName,String pConstant){
		cmd=pCmd;
		col_name=pColName;
		constant=pConstant;
	}
}