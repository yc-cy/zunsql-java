package npu.zunsql.treemng;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ed on 2017/10/30.
 */

//结点的关键字类
public class Row implements Serializable
{
    // 每个关键字类包含多个数值，多个数值存储于一个列表中
    protected List<Cell> cellList = new ArrayList<Cell>();

    //用多个数值创建关键字实例
    protected Row(List<String> SList)
    {
        for(int i = 0; i < SList.size(); i++)
        {
            cellList.add(new Cell(SList.get(i)));
        }
    }

    //获取关键字实例保存的数值列表
    protected List<String> getStringList()
    {
        List<String> SList = new ArrayList<String>();
        for(int i = 0; i < cellList.size(); i++)
        {
            SList.add(cellList.get(i).getValue_s());
        }
        return SList;
    }

    //获取关键子实例保存数据的第几个数值
    protected Cell getCell(int array)
    {

        return cellList.get(array);
}
}
