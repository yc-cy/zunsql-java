package npu.zunsql.cache;

//文件头类
public class FileHeader
{
    protected int version;
    public static int magicNum;
    public static final int fileHeaderSize = 1024;

    FileHeader(int version, int magicNum)
    {
        this.version = version;
        FileHeader.magicNum = magicNum;
    }
    boolean isValid()
    {
        //指定两个识别标识，若不相同则文件无效
        //？？标识待完善
        if(this.version == 1 && FileHeader.magicNum == 123)
            return true;
        else
            return false;
    }
}
