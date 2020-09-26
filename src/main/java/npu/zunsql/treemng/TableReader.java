package npu.zunsql.treemng;

import java.io.IOException;
import java.util.List;

/**
 * Created by Ed on 2017/10/28.
 */
//数据表读取类
public interface TableReader
{
    //获取表列名
    public abstract List<String> getColumnsName();
    //获取表列类型
    public abstract List<BasicType> getColumnsType();
    //获取表游标
    public abstract Cursor createCursor(Transaction thistran) throws IOException, ClassNotFoundException;
}


