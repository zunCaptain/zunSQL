package npu.zunsql.virenv;

class AttrInstance
{
	String type;
    String value;
    String attrName;

    public AttrInstance(String pAttrName,String pType,String pValue)
	{
		value=pValue;
		type=pType;
		attrName=pAttrName;
	}
    public String getAttrName()
    {
        return attrName;
    }
    public String getValue()
    {
        return value;
    }
}