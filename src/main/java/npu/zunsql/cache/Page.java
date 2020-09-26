package npu.zunsql.cache;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

//页面类，读写数据库文件的基本单位
public class Page implements Serializable
{

    //序列化说明：
	//serialVersionUID——序列化识别标识，每一个序列化类都有
    //在反序列化时，便于文件比对
    //序列化生成方式：
    //serialVersionUID有两种显示的生成方式：
    //一是默认的1L，比如：private static final long serialVersionUID = 1L;
    //二是根据包名，类名，继承关系，非私有的方法和属性，以及参数，返回值等诸多因子计算得出的，极度复杂生成的一个64位的哈希字段。
    //基本上计算出来的这个值是唯一的。比如：private static final long  serialVersionUID = xxxxL;
	private static final long serialVersionUID = 1L;



	public static final int PAGE_SIZE = 1024;
	//已开启页面数，用于新页面生成
    //已开启页面存在被修改而为空页的情况
	protected static int pageCount = 0;
	//存储未被使用的数据页序号的整型数组
    protected static List<Integer> unusedID =  new ArrayList<Integer>();
    //当前页面id
    protected int pageID;
    protected ByteBuffer pageBuffer = null;

    //仅buffer参数构造器
    public Page(ByteBuffer buffer)
    {
        if(Page.unusedID.isEmpty())
            //未使用页面为空，说明此时无页面，或页面按序号被使用
            this.pageID = pageCount++;
        else
        {
            //页面不为空时，获取未使用列表里的第一个页面
            this.pageID = Page.unusedID.indexOf(0);
            Page.unusedID.remove(0);
        }
        this.pageBuffer = buffer;
    }

    //ID+buffer参数构造器
    public Page(int pageID, ByteBuffer buffer)
    {
        this.pageID = pageID;
        this.pageBuffer = buffer;
    }

    //page构造器
    public Page(Page page)
    {
        this.pageID = page.pageID;
        //建立同等大小缓存空间
        ByteBuffer tempBuffer = ByteBuffer.allocate(page.pageBuffer.capacity());
        //导入buffer字节内容
        tempBuffer.put(page.pageBuffer);
        this.pageBuffer = tempBuffer;
    }

    public int getPageID()
    {
        return this.pageID;
    }

    public ByteBuffer getPageBuffer()
    {
        return this.pageBuffer;
    }
}