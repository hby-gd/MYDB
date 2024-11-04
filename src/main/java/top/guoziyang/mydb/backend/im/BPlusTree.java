package top.guoziyang.mydb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.im.Node.InsertAndSplitRes;
import top.guoziyang.mydb.backend.im.Node.LeafSearchRangeRes;
import top.guoziyang.mydb.backend.im.Node.SearchNextRes;
import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;
import top.guoziyang.mydb.backend.utils.Parser;

public class BPlusTree {
    DataManager dm;
    long bootUid;
    DataItem bootDataItem;
    Lock bootLock;

    /**
     * 创建一个 B+树 的根
     * @param dm    数据管理器
     * @return  B+树 的 bootUid
     * @throws Exception
     */
    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();  // 创建空的节点数组
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);    // 将根节点插入数据管理器中，返回根节点的UID
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid)); //保存根节点的UID，并插入，返回其索引位置
    }

    /**
     *
     * @param bootUid  B+树的唯一启动表示符
     * @param dm    数据管理器
     * @return  返回一颗 B+树
     * @throws Exception
     */
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid); // 读取 UID 对应的数据项
        assert bootDataItem != null;

        // 构建一颗新的 B+ 树实例
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;    // 设置树的 bootUid
        t.dm = dm;              // 保存数据管理器的引用到 B+树 中，便于数据管理
        t.bootDataItem = bootDataItem;  // 关联 bootUid 的数据项
        t.bootLock = new ReentrantLock();   // 初始化 B+树 的全局锁
        return t;
    }

    /**
     * 返回 B+树 的根节点UID
     * @return
     */
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 更新根节点
     * @param left
     * @param right
     * @param rightKey
     * @throws Exception
     */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            // 构建新的根节点信息
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            // dm插入新的跟节点信息，获取 rootuid
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            // 记录旧的根节点信息
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            // 更新根节点数据项的 根节点uid信息
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            // 提交数据更新（记录更新日志）
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    /**
     * 查找指定键的叶子节点的 uid
     * @param nodeUid
     * @param key
     * @return
     * @throws Exception
     */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid); // 从当前节点中获取 nodeUid 对应的节点
        boolean isLeaf = node.isLeaf();
        node.release();

        //判断是否为叶子节点
        if(isLeaf) {
            return nodeUid;
        } else {
            // 非叶子节点，寻找下一个节点Uid
            long next = searchNext(nodeUid, key);
            // 继续查找键值，直到找到叶子节点为止
            return searchLeaf(next, key);
        }
    }

    /**
     * 寻找给定键在 B+树 中的下一个节点uid
     * @param nodeUid
     * @param key
     * @return
     * @throws Exception
     */
    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            // 读取当前节点信息
            Node node = Node.loadNode(this, nodeUid);
            // 从当前节点的子节点中查找 键为 key 的子节点
            SearchNextRes res = node.searchNext(key);
            node.release();
            // 找到的话返回对应的子节点 id
            if(res.uid != 0) return res.uid;

            // 没找到，旧返回兄弟节点，从兄弟节点的子节点中查找
            nodeUid = res.siblingUid;
        }
    }

    /**
     * 查找键值为指定 key 的节点列表
     * @param key
     * @return
     * @throws Exception
     */
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    /**
     *
     * @param leftKey
     * @param rightKey
     * @return
     * @throws Exception
     */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();   // 获取根节点信息
        long leafUid = searchLeaf(rootUid, leftKey);    // 找到根节点中，leftKey 后的叶子节点 ID
        List<Long> uids = new ArrayList<>();
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);   // 读取叶子节点的信息
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey); //获取 leftKey 与 rightKey 间的所有节点信息
            leaf.release();
            uids.addAll(res.uids);
            // 判断是否从当前节点中获取全部的结果
            if(res.siblingUid == 0) {
                break;
            } else {
                // 从下一个节点的子节点中继续查
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    /**
     * 向当前 B+树 插入key、uid
     * @param key   键
     * @param uid   值的索引
     * @throws Exception
     */
    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    class InsertRes {
        long newNode, newKey;
    }

    /**
     * 向指定节点插入 key uid
     * @param nodeUid
     * @param uid
     * @param key
     * @return
     * @throws Exception
     */
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid); // 获取指定 node 对象
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        // 若为叶子节点
        if(isLeaf) {
            // 向节点中插入 uid 和 key
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            // 在当前节点的子节点下查找 值为 key 的下一个节点
            long next = searchNext(nodeUid, key);
            // 向子节点插入数据
            InsertRes ir = insert(next, uid, key);

            // 判断是否分裂
            if(ir.newNode != 0) {

                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    /**
     * 向指定节点插入 uid、key
     * @param nodeUid
     * @param uid
     * @param key
     * @return
     * @throws Exception
     */
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();

            // 判断是否分裂，若分裂，就向新分裂的节点中插入数据
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                // 若未分裂，记录新的节点信息及其Key值
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }


    public void close() {
        bootDataItem.release();
    }
}
