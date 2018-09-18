package npu.zunsql.treemng;

import java.io.Serializable;

/**
 * Created by Ed on 2017/10/30.
 */
public class Cell implements Serializable
{
    private String sValue;

    protected Cell(String  givenValue)
    {
        sValue = givenValue;
    }

    protected Cell(Integer  givenValue)
    {
        sValue = givenValue.toString();
    }

    protected Cell(Double  givenValue)
    {
        sValue = givenValue.toString();
    }

    protected boolean bigerThan(Cell cell)
    {
        return sValue.compareTo(cell.getValue_s()) > 0;
    }

    protected boolean equalTo(Cell cell)
    {
         return sValue.contentEquals(cell.getValue_s());

    }

    // 返回本单元的String值
    // 输入参数：无
    // 输出参数：String类型。
    protected String getValue_s()
    {
        return sValue;
    }
    protected Integer getValue_i()
    {
        return Integer.valueOf(sValue);
    }
    protected Double getValue_d()
    {
        return Double.valueOf(sValue);
    }
}
