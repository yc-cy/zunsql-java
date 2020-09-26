package npu.zunsql.treemng;

import npu.zunsql.cache.CacheMgr;
import npu.zunsql.cache.Page;
import sun.lwawt.macosx.CSystemTray;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
/**
 * Created by WQT on 2017/11/6.
 */

// 本Node类用于组织B树内部结构
// 每个Node包含一个Row列表和一个SonNode列表，其中SonNode列表除初始化时外，始终比Row列表多一个。
public class Node {

    // 需要存入page
    // 每个节点包含不少于M/2+1，不超过M+1的SonNode。
    private List<Integer> sonNodeList;

    // 需要存入page
    //  父节点
    private int fatherNodeID;

    // 需要存入page
    // 表示本节点在其上层父节点的子结点列表中的序号
    private int order;

    // 需要存入page
    // 每个节点包含不少于M/2，不超过M的Row。（关键字)
    private List<Row> rowList;

    // 用于表示本树为几阶B树。
    // 初始化为3阶
    public static int M = 3;

    // 用于操作存储page
    private CacheMgr cacheManager;

    // 每个Node表示一个Page
    private Page pageOne;

    // 用被赋予诸属性的页创建结点实例
    // 用该页存储的信息给新结点实例的各属性赋值
    protected Node(int thisPageID, CacheMgr cacheManager, Transaction thisTran) throws IOException, ClassNotFoundException {
        this.cacheManager = cacheManager;
        //此处的页面获取涉及到缓冲区的使用
        pageOne = this.cacheManager.readPage(thisTran.tranNum, thisPageID);


        //获取特定页面对应的存储空间
        ByteBuffer thisBufer = pageOne.getPageBuffer();
        byte [] bytes=new byte[Page.PAGE_SIZE] ;
        thisBufer.rewind();

        //将存储区的字节内容导入到字节数组中
        thisBufer.get(bytes,0,thisBufer.remaining());

        ByteArrayInputStream byteTable=new ByteArrayInputStream(bytes);
        ObjectInputStream objTable=new ObjectInputStream(byteTable);

        //从页信息的流对象里获取B数关联信息
        this.sonNodeList=(List<Integer>)objTable.readObject();
        this.fatherNodeID=(int) objTable.readObject();
        this.order=(int)objTable.readObject();
        this.rowList=(List<Row>)objTable.readObject();

    }

    // 根据Node的属性构造Node。
    private Node(List<Row> thisRowList, List<Integer> thisSonList, int thisOrder, CacheMgr cacheManager, Transaction thisTran) throws IOException, ClassNotFoundException {
        ByteBuffer buffer = ByteBuffer.allocate(Page.PAGE_SIZE);

        // 新结点创建，需要给新结点加入：
        // 1.存储多个关键字的列表
        // 2.存储子结点的列表
        // 3.父结点暂留空
        // 4.页面管理实例
        // 5.每个结点对应的一个存储页面
        rowList = thisRowList;
        sonNodeList = thisSonList;
        fatherNodeID = -1;
        this.cacheManager = cacheManager;
        pageOne = new Page(buffer);

        // 将父结点信息加入到每个儿子结点
        // 包括父结点ID和子结点在父结点中的序号
        for (int i = 0; i < sonNodeList.size(); i++)
        {
            Node sonNode = new Node(sonNodeList.get(i),cacheManager,thisTran);
            sonNode.setFather(pageOne.getPageID(),thisTran);
            sonNode.setOrder(i,thisTran);
        }

        // 维护自身排位信息。
        order = thisOrder;
        // 序列化信息至buffer
        intoBytes(thisTran);

    }


    // 重新录入结点信息到结点对应页面
    protected void intoBytes (Transaction thisTran) throws IOException {
        byte [] bytes=new byte[Page.PAGE_SIZE] ;
        ByteArrayOutputStream byt=new ByteArrayOutputStream();
        ObjectOutputStream obj=new ObjectOutputStream(byt);

        //将结点关联信息写入数据流
        obj.writeObject(sonNodeList);
        obj.writeObject(fatherNodeID);
        obj.writeObject(order);
        obj.writeObject(rowList);
        bytes=byt.toByteArray();

        //将已写好的数据流信息写入页存储区
        pageOne.getPageBuffer().rewind();
        pageOne.getPageBuffer().put(bytes);
        //将写好的页面加入到事务页面管理中
        cacheManager.writePage(thisTran.tranNum,pageOne);
        //thisTran.Commit();
    }



    //设置父结点ID，并重新录入
    private boolean setFather(int ID,Transaction thisTran) throws IOException {
        fatherNodeID = ID;

        // 维护page信息
        intoBytes(thisTran);

        return true;
    }


    //设置子结点在父结点中的顺序，并重新录入
    private boolean setOrder(int or, Transaction thisTran) throws IOException {
        order = or;

        // 维护page信息
        intoBytes(thisTran);

        return true;
    }

    //新增合并操作，用于解决在向父结点借用时，父结点关键字个数不够的情况
    //本结点为父结点，函数用于对本结点的子结点进行右三结点合并
    private boolean rightMergeNode(int sonOrder, Transaction thisTran) throws IOException, ClassNotFoundException {

        //获取待调整子结点
        Node thisSonNode = new Node(sonNodeList.get(sonOrder),cacheManager,thisTran);

        //若右邻接结点存在，则进行关键字合并操作
        //获取右兄弟结点
        Node rightSonNode = new Node(sonNodeList.get(sonOrder + 1), cacheManager, thisTran);
        //获取父结点（即本结点）右关键字
        Row fatherRightRow = rowList.get(sonOrder);
        //将父结点右关键字插入到待调整结点末尾
        thisSonNode.insertRow(fatherRightRow, thisTran);
        //删除对应位置父结点
        rowList.remove(sonOrder);

        //将右邻接结点的全部关键字依次插入到待调整子结点的末尾
        thisSonNode.rowList.addAll(subRowList(rightSonNode.rowList, 0, rightSonNode.rowList.size() - 2));
        //最后一个关键字用insert函数插入，用于判断全部插入完成后关键字个数是否越阶
        thisSonNode.insertRow(rightSonNode.rowList.get(rightSonNode.rowList.size()-1), thisTran);

        //删除父结点记录中的右子结点
        this.sonNodeList.remove(sonOrder + 1);

        //判断字结点是否为终端结点
        if ((thisSonNode.sonNodeList != null) || (thisSonNode.sonNodeList.size() != 0))
        {
            //若不为终端结点
            //将右兄弟结点的全部子子结点加入到待调整子结点的子子结点列表中
            thisSonNode.sonNodeList.addAll(rightSonNode.sonNodeList);

            //销毁右子结点
            rightSonNode = null;
        }
        return true;
    }

    //左三结点合并
    private boolean leftMergeNode(int sonOrder, Transaction thisTran) throws IOException, ClassNotFoundException {

        //获取待调整子结点
        Node thisSonNode = new Node(sonNodeList.get(sonOrder),cacheManager,thisTran);

        //获取左兄弟结点
        Node leftSonNode = new Node(sonNodeList.get(sonOrder - 1),cacheManager,thisTran);
        //获取父结点左关键字
        Row fatherRightRow = rowList.get(sonOrder-1);
        //将父结点右关键字插入到待调整结点开头
        thisSonNode.insertRow(fatherRightRow, thisTran);
        //删除对应位置父结点
        rowList.remove(sonOrder);

        //将左邻接结点的全部关键字依次插入到待调整子结点的开头
        leftSonNode.rowList.addAll(subRowList(thisSonNode.rowList, 0, thisSonNode.rowList.size() - 2));
        //最后一个关键字用insert函数插入，用于判断全部插入完成后关键字个数是否越阶
        thisSonNode.insertRow(leftSonNode.rowList.get(leftSonNode.rowList.size()-1), thisTran);

        //删除父结点记录中的左子结点
        this.sonNodeList.remove(sonOrder - 1);

        //判断字结点是否为终端结点
        if ((thisSonNode.sonNodeList != null) || (thisSonNode.sonNodeList.size() != 0))
        {
            //若不为终端结点
            //将右兄弟结点的全部子子结点加入到待调整子结点的子子结点列表中
            thisSonNode.sonNodeList.addAll(leftSonNode.sonNodeList);

            //销毁右子结点
            leftSonNode = null;
        }

        return true;
    }




    // B树结点分裂操作，返回拆分后的右结点
    // 将一个结点分为左右两个结点（除跟结点）
    private Node devideNode(Transaction thisTran) throws IOException, ClassNotFoundException {

        List<Row> rightRow;
        List<Integer> rightNode;

        // 将原本的子结点按阶树大小分成两半
        // 在之后的分裂操作中，（M/2+1）这个关键字将会被提到上一个层级，因此该结点移出子结点
        rightRow = subRowList(rowList,M/2 + 2, M-1);
        rowList = subRowList(rowList,0, M/2); //左结点

        // 若为叶子结点，则仅分离关键字而不分离所属子结点
        if(sonNodeList.size() == 0)
        {
            rightNode = new ArrayList<>();
        }
        //不为叶子结点时，将对应子结点序号进行划分
        else
        {
            //？？？根据B树分裂情况，当(M/2+1)关键字上移时，(M/2+1)对应的子结点变为新右结点开头的第一个子结点
            //     与（M/2+1）子结点对应的是在关键字列表中新增的，用于记录该结点关键字个数的值
            //故此处对其进行修改：

            rightNode = subNodeList(sonNodeList,M/2 + 1,M);
            sonNodeList = subNodeList(sonNodeList,0, M/2);

            //原代码为：
            //  rightNode = subNodeList(sonNodeList,M/2 + 1,M);
            //  sonNodeList = subNodeList(sonNodeList,0, M/2 + 1);
        }

        //录入新信息
        intoBytes(thisTran);

        //返回新生成的右结点，该结点的序号为父结点所属子结点列表最大序号+1
        return new Node(rightRow, rightNode, order + 1, cacheManager, thisTran);
    }

    // 分裂根节点
    // 此函数已假设根结点关键字个数超限
    private boolean rootDevideNode(Transaction thisTran) throws IOException, ClassNotFoundException {

        List<Row> leftRow;
        List<Row> rightRow;
        List<Integer> leftNode;
        List<Integer> rightNode;

        //左结点部分
        //左结点分配（0，M/2-1）范围的关键字
        leftRow = subRowList(rowList,0,M/2-1);

        //判断根结点是否存在子结点
        if(sonNodeList.size() == 0)
        {
            //若不存在左子结点，则创建一个
            leftNode = new ArrayList<>();
        }
        else
        {
            //左结点分配根结点处于（0，M/2）的子结点
            //代表左结点的子结点列表
            leftNode = subNodeList(sonNodeList,0,M/2);
        }
        //建立新左子结点
        Node newLeftNode = new Node(leftRow, leftNode, 0, cacheManager, thisTran);


        //右结点部分
        //右结点分配（M/2+1,M-1)范围的关键字
        rightRow = subRowList(rowList,M/2 + 1, M-1);
        // 判断根结点是否存在子结点
        // 由于B树的性质，若若又一个子结点存在，那么必然有N-1个子结点存在(N为本结点的关键字个数)
        // 因此此处判断是否为0即可达到目的
        if(sonNodeList.size() == 0)
        {
            rightNode = new ArrayList<>();
        }
        else
        {
            //右结点分配根结点处于（M/2+1，M）位置的子结点
            rightNode =subNodeList(sonNodeList,M/2 + 1, M);
        }
        //建立新右子结点
        Node newRightNode = new Node(rightRow, rightNode,1, cacheManager, thisTran);

        //原结点留下位于M/2处关键字，作为新的根结点
        rowList = subRowList(rowList,M/2, M/2);

        //创建根结点新子结点列表
        List<Integer> newSonList = new ArrayList<>();

        //将新左子结点加入到根结点
        newSonList.add(newLeftNode.pageOne.getPageID());
        //将新右子结点加入到根结点
        newSonList.add(newRightNode.pageOne.getPageID());

        //覆盖原结点的子结点列表，新的子结点为新生成的左右子结点
        sonNodeList = newSonList;

        //调整左子结点信息
        newLeftNode.setFather(this.pageOne.getPageID(), thisTran);
        newLeftNode.setOrder(0, thisTran);
        //调整右子结点信息
        newRightNode.setFather(this.pageOne.getPageID(), thisTran);
        newRightNode.setOrder(1, thisTran);

        //信息更新
        intoBytes(thisTran);
        return true;
    }

    //获取子结点序号列表的子列表
    private List<Integer> subNodeList(List<Integer>list, int a, int b)
    {
        List<Integer> ret = new ArrayList<Integer>();
        for(int i = a; i <= b; i++)
        {
            ret.add(list.get(i));
        }
        return ret;
    }

    //获取关键字列表的子列表
    private List<Row> subRowList(List<Row>list, int a, int b)
    {
        List<Row> ret = new ArrayList<>();
        for(int i = a; i <= b; i++)
        {
            ret.add(list.get(i));
        }
        return ret;
    }

    //判断是否有右兄弟结点
    private boolean ifHaveRightSonNode(int order)
    {
        if (order <= sonNodeList.size() - 2)
        {
            return true;
        }
        else
        {
            return false;
        }
    }


    //此函数仅用于待删除结点已备份的情况
    //用于父结点(本结点)删除子结点的情况
    //调用后直接将该位置结点删除
    private boolean justDelete(int sonOrder, Transaction thisTran) throws IOException, ClassNotFoundException{

        // 判断待删除结点的序号是否有效
        if ((sonOrder <= sonNodeList.size() - 1) && (sonOrder >= 0))
        {
            //若有效，则直接删除
            sonNodeList.remove(sonOrder);
            return true;
        }
        //若子结点序号无效，则删除失败
        return false;
    }


    //获取右兄弟结点的第一个关键字，并加入到待调整结点中
    //右兄弟结点有足够关键字关键字个才能获取
    private boolean getRightRow(int sonOrder, Transaction thisTran) throws IOException, ClassNotFoundException {

        //获取待调整子结点
        Node thisSonNode = new Node(sonNodeList.get(sonOrder),cacheManager,thisTran);

        //获取待调整子结点右邻接的子结点
        Node rightSonNode = new Node(sonNodeList.get(sonOrder + 1), cacheManager, thisTran);

        //判断右兄弟结点关键字个数
        if (rightSonNode.rowList.size() > M/2) {
            //右结点关键字数足够
            //将父结点对应i位置的右关键字插入到待调整子结点中
            //根据B树性质，在同一列的i处对应的右关键字，若根据大小排列则必然处于第i处子结点关键字的末尾
            //但由于insertRow还存有对关键字个数的后续判定，因此不作改动
            thisSonNode.insertRow(rowList.get(sonOrder), thisTran);


            //将右邻接子结点的第一个关键字覆盖已插入到待调整子结点的关键字i位置
            rowList.set(sonOrder, rightSonNode.getFirstRow(thisTran));
            //删除右邻接子结点的第一个关键字
            //由于上面已进行了关键字个数判断，此处直接删除即可
            rightSonNode.rowList.remove(0);


            // 此处需考虑是否处于终端结点的情况
            // 若处于终端结点，则直接删除右邻接子结点的第一个关键字即可(完成上述最后一步即可)

            //  若不为终端结点，则需要将右邻接子结点的第一个子子结点移接到待调整子结点的最后一个子子结点位置
            if ((rightSonNode.sonNodeList != null) || (rightSonNode.sonNodeList.size() != 0)) {
                //将右邻接子结点的第一个子子结点移接到待调整子结点的最后一个子子结点位置
                thisSonNode.sonNodeList.add(rightSonNode.sonNodeList.get(0));
                //删除右邻接子结点的第一个子子结点
                rightSonNode.justDelete(0, thisTran);
            }
            return true;
        }
        else
        {
            //右结点关键字个数不够时，则不进行关键字转移
            return false;
        }
    }

    //获取左兄弟结点的第一个关键字，并加入到待调整结点中
    private boolean getLeftRow(int sonOrder, Transaction thisTran) throws IOException, ClassNotFoundException {

        //获取待调整子结点
        Node thisSonNode = new Node(sonNodeList.get(sonOrder),cacheManager,thisTran);

        //获取左兄弟结点
        Node leftSonNode = new Node(sonNodeList.get(sonOrder - 1),cacheManager,thisTran);

        //判断左兄弟结点关键字个数
        if (leftSonNode.rowList.size() > M/2) {

            //左结点操作与上述右结点操作相同
            //将父结点对应i位置的左关键字插入到待调整子结点中
            thisSonNode.insertRow(rowList.get(sonOrder - 1), thisTran);
            //将左邻接子结点的最后一个关键字覆盖已插入到待调整子结点的关键字i位置
            this.rowList.set(sonOrder - 1, leftSonNode.getLastRow(thisTran));
            //删除左邻接子结点的最后一个关键字
            //由于上面进行了关键字个数判断，此处直接删除即可
            leftSonNode.rowList.remove(leftSonNode.rowList.size() - 1);

            //  若处于终端结点，则直接删除右邻接子结点的第一个关键字即可
            //  若不为终端结点，则需要将右邻接子结点的第一个子子结点移接到待调整子结点的最后一个子子结点位置
            if ((leftSonNode.sonNodeList != null) || (leftSonNode.sonNodeList.size() != 0)) {
                //将左邻接子结点的最后一个子子结点移接到待调整子结点的第一个子子结点位置
                thisSonNode.sonNodeList.add(0, leftSonNode.sonNodeList.get(sonNodeList.size() - 1));
                //删除左邻接子结点的最后一个子子结点
                leftSonNode.justDelete(sonNodeList.size() - 1, thisTran);
            }
            return true;
        }
        else
        {
            //左结点关键字个数不够时，则不进行关键字转移
            return false;
        }
    }


    // 调整本节点sonOrder位置的子结点，使该子结点的关键字个数恢复至M/2
    // 要求，待调整结点必须具有父结点
    private boolean adjustNode(int sonOrder, Transaction thisTran) throws IOException, ClassNotFoundException {

        //待调整子结点是否有邻接右结点
        boolean if_have_right = ifHaveRightSonNode(sonOrder);


        // 直接向兄弟结点借关键字部分
        if (if_have_right && this.getRightRow(sonOrder, thisTran))
        {
            //满足有右兄弟结点且已向右借关键字
            intoBytes(thisTran);
            return true;
        }
        else if (getLeftRow(sonOrder, thisTran))
        {
            //已向左借关键字
            intoBytes(thisTran);
            return true;
        }


        //向父结点借关键字部分
        //若没有可支援的兄弟结点，需要向父结点请求加入关键字
        //设置默认与右邻接结点，父结点右关键字合并，不存在时再进行左结点合并
        if (if_have_right)
        {
            //右邻接子结点存在，则在本结点进行右合并操作
            this.rightMergeNode(sonOrder, thisTran);
        }
        else {
            //由B树性质可知，右若邻接结点不存在，则左邻接结点必然存在
            this.leftMergeNode(sonOrder, thisTran);
        }


        //连锁反应的处理
        //判断是否为根结点
        if (this.fatherNodeID == -1)
        {
            //在right/leftMergeNode函数中已经对根结点合并的两种情况进行了考虑，因此直接返回true
            return true;
        }
        else
        {
            //若不为根结点，则判断父结点关键字是否足够
            if (this.rowList.size() >= M/2)
            {
                //若足够，则删除成功
                return true;
            }
            else {
                //若不够，则递归调用adjustNode，直至所有父节点关键字个数都>=M/2为止
                return new Node(fatherNodeID, cacheManager, thisTran).adjustNode(order, thisTran);
            }
        }
    }

    // 插入关键字，和分裂后的新右结点
    // 此函数仅用于子结点越阶，子结点中间关键字上提插入至父结点的情况
    private boolean addNode(Row row, Node node, Transaction thisTran) throws IOException, ClassNotFoundException {


        //第一部分，先插入
        // 记录是否已插入新关键字这个状态，若为true则下方循环终止
        boolean addOrNot = false;
        //遍历本结点关键字
        for (int i = 0; i < rowList.size(); i++)
        {
            //获取本结点第i处的关键字
            Row thisRow = rowList.get(i);
            //获取本结点第i处的子结点
            Node thisNode = new Node(sonNodeList.get(i), cacheManager, thisTran);

            //判断第i处关键字值是否大于待插入关键字值
            if (!addOrNot && thisRow.getCell(0).bigerThan(row.getCell(0)))
            {
                //若新键值小于原结点第i处键值，则将新关键字插入到原结点i位置
                rowList.add(i, row);

                //对应子结点的新右结点将插入子结点列表的位置i+1
                //从i+1开始调整每个子结点的order属性
                for (int index = i+1; index <= this.sonNodeList.size() - 1; index++)
                {
                    //获取第index处的子结点
                    Node tempNode = new Node(this.sonNodeList.get(index), cacheManager, thisTran);
                    //调整index位置子结点的order属性
                    tempNode.setOrder(index+1, thisTran);
                }

                // 子结点分裂后生成的左结点与原位置相同而不做更改
                // 子结点第i+1处加入新生成的右结点
                sonNodeList.add(i+1, node.pageOne.getPageID());
                //给新生成右结点设置父结点
                node.setFather(pageOne.getPageID(), thisTran);
                //给新生成右结点设置子结点序号
                node.setOrder(i+1, thisTran);

                intoBytes(thisTran);
                //表示已插入了关键字并调整了子结点
                addOrNot = true;
            }
        }
        if (!addOrNot)
        {
            // 若本结点不存在比其大的键值，则将关键字添加至末尾。
            rowList.add(row);
            // 在末尾加入新键值
            sonNodeList.add(node.pageOne.getPageID());

            // 此处修正新右结点属性值：
            // 给新右结点设置父结点（即为本结点）
            node.setFather(this.pageOne.getPageID(), thisTran);
            // 给新右子结点设置子结点序号
            node.setOrder(this.sonNodeList.size(), thisTran);

            intoBytes(thisTran);
        }


        //第二部分，判断关键字个数是否越阶，并附加越阶处理
        //判断本结点关键字个数，B树关键字个数范围(M/2,M-1)
        if (rowList.size() < M)
        {
            // 当未超出长度时，插入完毕。
            return true;
        }
        else
        {
            // 超出长度时，进行单元分裂
            return this.solveOverstep(thisTran);
        }
    }


    protected List<Integer> getSonNodeList()
    {
        return sonNodeList;

    }

    protected int getFatherNodeID()
    {
        return fatherNodeID;
    }

    protected Node getSpecialSonNode(int sonOrder,Transaction thisTran) throws IOException, ClassNotFoundException {
        return new Node(sonNodeList.get(sonOrder),cacheManager,thisTran);
    }

    protected List<Row> getRowList()
    {
        return rowList;
    }
    protected Node getFatherNode(Transaction thisTran) throws IOException, ClassNotFoundException
    {
        return new Node(fatherNodeID,cacheManager,thisTran);
    }
    protected int getOrder()
    {
        return order;
    }


    //在本结点按大小顺序插入关键字
    //B树的插入需要保证在终端结点上进行
    //注意：由于该函数为递归插入，因此不能保证新关键字插入于本结点
    public boolean insertRow(Row row,Transaction thisTran) throws IOException, ClassNotFoundException {

        //第一部分为插入点判断
        boolean insertOrNot = false;
        int insertNumber = 0;

        for (int i = 0; i < rowList.size(); i++)
        {
            if (rowList.get(i).getCell(0).equalTo(row.getCell(0)))
            {
                //如果有相同的键值，则插入失败，返回false
                return false;
            }
            else if (rowList.get(i).getCell(0).bigerThan(row.getCell(0)))
            {
                //  若原关键字比新增关键字大
                //  则将新增关键字插入到对应i处
                insertNumber = i;

                //此标识表示是否插入到内部
                insertOrNot = true;
                break;
            }
        }
        if (!insertOrNot)
        {
            //没有插入到内部，则放到最后面
            insertNumber = rowList.size();
        }


        //第二部分
        //终端结点判断，该判断决定是否进行插入操作
        //若不为终端结点，则从上面找到的插入序号对应关键字位置开始，对该关键字对应的子结点进行递归遍历
        if ((sonNodeList == null)|| (sonNodeList.size() == 0))
        {
            //若本结点为终端结点，则不考虑子结点调整问题
            rowList.add(insertNumber, row);
            intoBytes(thisTran);
            //满足阶数限制，则插入成功
            if (rowList.size() <= M)
            {
                return true;
            }
            else
            {
                //关键字大于阶数，进行分裂操作
                return this.solveOverstep(thisTran);
            }
        }
        else
        {
            //若不为终端结点,则递归地对子结点进行操作，直到在终端结点处才进行插入操作
            return new Node(sonNodeList.get(insertNumber),cacheManager,thisTran).insertRow(row,thisTran);
        }
    }

    //将父结点关键字插入到子结点
    //子结点为本结点
    private boolean insertFromFatherNode(Row row, Transaction thisTran) throws IOException, ClassNotFoundException {

        //判断是否已插入，初始为未插入
        boolean insertOrNot = false;

        for (int i = 0; i < rowList.size(); i++)
        {
            if (rowList.get(i).getCell(0).equalTo(row.getCell(0)))
            {
                //如果有相同的键值，则插入失败，返回false
                System.out.println("insertFromFatherNode Error: 待插入关键字于原关键字键值相同!");
            }
            else if (rowList.get(i).getCell(0).bigerThan(row.getCell(0)))
            {
                //  若原关键字比新增关键字大
                //  则将新增关键字插入到对应i处
                rowList.add(i, row);

                //表示已插入成功
                insertOrNot = true;
                break;
            }
        }
        if (!insertOrNot)
        {
            //没有插入到内部，则放到最后面
            rowList.add(rowList.size(),row);
        }

        intoBytes(thisTran);
        return true;
    }


    //关键字个数越阶处理
    private boolean solveOverstep(Transaction thisTran) throws IOException, ClassNotFoundException {

        //判断是否为根结点
        if (this.fatherNodeID < 0)
        {
            //若为根结点，则调用跟结点分裂函数
            return this.rootDevideNode(thisTran);
        }
        else
        {
            //不为根结点，且关键字超限制时，将M/2+1位置的结点上提到父结点，本结点分成左右两部分，并将新生成的右结点插入到父结点子结点列表中
            return new Node(this.fatherNodeID, this.cacheManager, thisTran).addNode(this.rowList.get(M/2+1),devideNode(thisTran),thisTran);
        }
    }



    public boolean drop(Transaction thisTran)
    {
        // TODO:递归清理所有节点
        return true;
    }

    //删除本结点某关键字
    //由于无法判断两个关键字之间是否相等，所以用关键字类中值列表的第一个数值作为判断标准
    public boolean deleteRow(Cell key,Transaction thisTran) throws IOException, ClassNotFoundException {

        boolean deleteOrNot = false;
        int deleteNumber = 0;

        //遍历本结点关键字
        for (int i = 0; i < rowList.size(); i++)
        {
            //获取第i处关键字
            Row thisRow = rowList.get(i);
            //判断第i处关键字中第0的值是否与所给key相同
            if (thisRow.getCell(0).equalTo(key))
            {
                if ((sonNodeList == null) || (sonNodeList.size() == 0))
                {
                    //若本结点为终端结点且第i处关键字中第0的值与所给key相同
                    //则将本结点第i处的关键字全部删除
                    rowList.remove(i);
                    // 维护page信息
                    intoBytes(thisTran);


                    //判断删除之后关键字个数
                    if (rowList.size() < M/2)
                    {
                        if (fatherNodeID < 0)
                        {
                        	if ((rowList.size() < 1) &&  (sonNodeList.size() < 1)) {
                        		return true;
                        	} else if (rowList.size() < 1)
                            {
                                rowList = new Node(sonNodeList.get(0),cacheManager,thisTran).rowList;
                                sonNodeList = new Node(sonNodeList.get(0),cacheManager,thisTran).sonNodeList;
                                // 维护page信息
                                intoBytes(thisTran);
                                return true;
                            }
                            else
                            {
                                return true;
                            }
                        }
                        else
                        {
                            return new Node(fatherNodeID,cacheManager,thisTran).adjustNode(order,thisTran);
                        }
                    }
                    else
                    {
                        return true;
                    }
                }
                else
                {
                    Row tempRow = getFirstRow(thisTran);
                    rowList.set(i, tempRow);
                    // 维护page信息
                    intoBytes(thisTran);
                    return new Node(sonNodeList.get(i + 1),cacheManager,thisTran).deleteRow(tempRow.getCell(0),thisTran);
                }
            }
            else if (thisRow.getCell(0).bigerThan(key))
            {
                deleteNumber = i;
                deleteOrNot = true;
                break;
            }
        }
        if (!deleteOrNot)
        {
            deleteNumber = rowList.size();
        }
        if ((sonNodeList == null) || (sonNodeList.size() == 0))
        {
            return false;
        }
        else
        {
            return new Node(sonNodeList.get(deleteNumber),cacheManager,thisTran).deleteRow(key,thisTran);
        }
    }

    //获取指定位置的关键字
    public Row getRow(int id)
    {
        if (rowList.size() > 0)
        {
            return rowList.get(id);
        }
        else
        {
            return null;
        }
    }


    //获取本结点的第一个关键字
    public Row getFirstRow(Transaction thisTran) throws IOException, ClassNotFoundException {

        //若本结点没有关键字，则返回null
    	if((rowList == null) || (rowList.size() == 0)) {
    		return null;
    	}

    	//若本结点为终端结点，则直接获取本结点第一个关键字
        if ((sonNodeList == null) || (sonNodeList.size() == 0))
        {
            return rowList.get(0);
        }
        //若本结点不为终端结点，则获取其子结点的第一个关键字
        //此处设置是为减少连锁反应带来的时间开销
        else
        {
            return new Node(sonNodeList.get(0),cacheManager,thisTran).getFirstRow(thisTran);
        }
    }


    //获取本结点最后一个键值
    public Row getLastRow(Transaction thisTran) throws IOException, ClassNotFoundException {

        if ((sonNodeList == null) || (sonNodeList.size() == 0))
        {
            //若为终端结点，则直接返回最后一个键值
            return rowList.get(rowList.size() - 1);
        }
        else
        {
            //若不为终端结点，则返回其子结点的最后一个键值
            return new Node(sonNodeList.get(sonNodeList.size() - 1),cacheManager,thisTran).getLastRow(thisTran);
        }
    }


    //获取特定位置的关键字
    public Row getSpecifyRow(Cell key,Transaction thisTran) throws IOException, ClassNotFoundException {
        int insertNumber = -1;

        //遍历本结点关键字
        for (int i = 0; i < rowList.size(); i++)
        {
            //获取第i个关键字
            Row thisRow = rowList.get(i);
            //通过比较关键字的第1个数值，判断两关键字是否相等
            if (thisRow.getCell(0).equalTo(key))
            {
                return thisRow;
            }
            else if (thisRow.getCell(0).bigerThan(key))
            {
                //若本结点第一次出现位于i位置的关键字大于给定key值
                //则给定key对应的关键字kennel位于关键字i对应子结点列表中的第i个子结点内
                if ((sonNodeList == null) || (sonNodeList.size() == 0))
                {
                    //若此时为终端结点，则说明不存在与给定key相等的关键字
                    //最后函数仍返回本结点第i处关键字
                    insertNumber = i;
                    break;
                }
                //若不为终端结点，则递归地对子结点调用本函数
                else
                {
                    return new Node(sonNodeList.get(i),cacheManager,thisTran).getSpecifyRow(key,thisTran);
                }
            }
        }
        if ((sonNodeList == null) || (sonNodeList.size() == 0))
        {
            //根据指定序号
            if (insertNumber > 0)
            {
                return rowList.get(insertNumber);
            }
            //如果本结点中不存在比key大的关键字，则返回null
            else
            {
                return null;
            }
        }
        else
        {
            return new Node(sonNodeList.get(sonNodeList.size() - 1),cacheManager,thisTran).getSpecifyRow(key,thisTran);
        }

    }

}
