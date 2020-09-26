package npu.zunsql.cache;

//import javax.print.DocFlavor;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;

public class Transaction
{
    //子事务个数
    protected static int transCount = 0;
    //当前事务子类的id
    protected int transID;
    //自定义事务文件后缀
    protected static final String SUFFIX_JOURNAL = "-journal";
    //是否可读写
    protected boolean WR;
    //读写锁
    protected ReadWriteLock lock;

    public Transaction(String s, ReadWriteLock lock)
    {
        //读写状态判定
        if(s == "r")
            this.WR = false;
        else
            this.WR = true;
        this.lock = lock;
        //每新增一个实例，根据现有的子事务数给id赋值
        this.transID = transCount++;
    }

    //事务开始
    public void begin()
    {
        if(this.WR)
        {
            //按子事务id名+事务后缀获取对应文件名的文件对象
            File journal = new File(Integer.toString(this.transID)+SUFFIX_JOURNAL);
            try
            {
                if(!journal.exists())
                {
                    Boolean bool = journal.createNewFile();
//                    System.out.println("File created: "+bool);
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            //为读写状态时，设置写锁
            this.lock.writeLock().lock();
        }
        //为读状态时，设置读锁
        else {
            this.lock.readLock().lock();
        }
    }

    //事务结束
    public void commit()
    {
        if(this.WR)
        {
            this.lock.writeLock().unlock();
        }
        else
        {
            this.lock.readLock().unlock();
        }

        //检测事务临时文件是否删除
        File journal = new File(Integer.toString(this.transID)+SUFFIX_JOURNAL);
        if(journal.exists()&&journal.isFile())
            journal.delete();

    }

    //？？？commit与rollback重复？
    //待修改
    public void rollback()
    {
        if(this.WR)
        {
            this.lock.writeLock().unlock();
        }
        else
        {
            this.lock.readLock().unlock();
        }
        
        File journal = new File(Integer.toString(this.transID)+SUFFIX_JOURNAL);
        if(journal.exists()&&journal.isFile())
            journal.delete();
    }
}
