package npu.zunsql.treemng;

import java.io.Serializable;

/**
 * Created by Ed on 2017/10/30.
 */

//保存一个数值的类，该数值可以是不同数据类型
public class Cell implements Serializable
{
    private String sValue;

    //三个构造类
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
        //按字典序比较字符串，若>0则前者字符unicode较大，或较长
        return sValue.compareTo(cell.getValue_s()) > 0;
    }

    protected boolean equalTo(Cell cell)
    {
        //字符串相等时返回true
         return sValue.contentEquals(cell.getValue_s());

    }

    //获取svalue的string类型
    protected String getValue_s()
    {
        return sValue;
    }
    //获取svalue的integer类型
    protected Integer getValue_i()
    {
        return Integer.valueOf(sValue);
    }
    //获取svalue的double类型
    protected Double getValue_d()
    {
        return Double.valueOf(sValue);
    }
}
