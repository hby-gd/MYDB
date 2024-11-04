package top.guoziyang.mydb.backend.im;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;
import top.guoziyang.mydb.backend.utils.Parser;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 *
 * LeafFlag：标记是否为叶子节点
 * KeyNumber：标记节点中 key 的数量
 * SiblingUid：存储兄弟节点在DM中的UID，实现节点间的连接
 * SonN KeyN：后续穿插的子节点，最后一个Key值为MAX_VALUE，方便查找
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0;    // 表示该节点是否为叶子节点
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1; // 标记节点中 key 的数量
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2; // 标记节点的兄弟节点的uid
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;   // 表示节点头部的大小的常量

    static final int BALANCE_NUMBER = 32;   // 节点平衡因子，一个节点最多包含32个 key
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2); // 节点大小

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    /**
     * 设置节点是否为叶子节点
     * @param raw
     * @param isLeaf
     */
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }

    /**
     * 判断节点是否是叶子节点
     * @param raw
     * @return
     */
    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    /**
     * 设置子节点个数
     * @param raw
     * @param noKeys
     */
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }

    /**
     * 获取子节点个数
     * @param raw
     * @return
     */
    static int getRawNoKeys(SubArray raw) {
        return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }

    /**
     * 设置兄弟节点的 uid
     * @param raw
     * @param sibling
     */
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }

    /**
     * 获取兄弟节点uid
     * @param raw
     * @return
     */
    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }

    /**
     * 设置第 k 个子节点的 uid
     * @param raw   节点原始字节数组
     * @param uid   要设置的uid
     * @param kth   子节点的索引
     */
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    /**
     * 获取第k个子节点的UID
     * @param raw
     * @param kth
     * @return
     */
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    /**
     * 设置第k个键的值
     * @param raw
     * @param key
     * @param kth
     */
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    /**
     * 获取第k个子节点的值
     * @param raw
     * @param kth
     * @return
     */
    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    /**
     * 从from节点的原始数组中复制一部分到 to 节点的原始数组中
     * @param from
     * @param to
     * @param kth
     */
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    /**
     * 将指定位置后的所有键和子节点后移一位
     * @param raw
     * @param kth
     */
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }

    /**
     * 生成一个根节点
     * @param left  左边界子节点
     * @param right 右边界子节点
     * @param key   初始键值
     * @return
     */
    static byte[] newRootRaw(long left, long right, long key)  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    /**
     * 生成一个空的节点
     * @return
     */
    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    /**
     * 读取 B+树 中 指定节点内容
     * @param bTree
     * @param uid
     * @return
     * @throws Exception
     */
    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    /**
     * 判断是否是叶子节点
     * @return
     */
    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    /**
     * 在B+树的节点中搜索下一个节点的方法
     * @param key
     * @return
     */
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            // 遍历当前节点的所有子节点 [left, right]
            for(int i = 0; i < noKeys; i ++) {
                long ik = getRawKthKey(raw, i); // 获取第 i 个 key 的值
                // 当前 ik 大于 指定 k，表示找到了下一个节点
                if(key < ik) {
                    res.uid = getRawKthSon(raw, i); // 设置下一个节点的 uid
                    res.siblingUid = 0;
                    return res;
                }
            }

            res.uid = 0;    // 没有找到下一个节点，设置 uid 为 0

            res.siblingUid = getRawSibling(raw);    // 设置兄弟节点的 uid 为当前节点的兄弟节点
            return res;

        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    /**
     * 在B+树的叶子节点中搜索一个键值范围的方法
     * 在当前节点进行范围查找，范围是 [leftKey, rightKey]，
     * 这里约定如果 rightKey 大于等于该节点的最大的 key, 则还同时返回兄弟节点的 UID，方便继续搜索下一个节点
     * @param leftKey
     * @param rightKey
     * @return
     */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw); // 获取节点中的键的数量
            int kth = 0;
            // 找到第一个大于或等于左键的键
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey) {
                    break;
                }
                kth ++;
            }

            List<Long> uids = new ArrayList<>();    // 创建一个列表，用于存储所有在键值范围内的子节点的UID
            // 遍历所有的键，将所有小于或等于右键的键对应的子节点的UID添加到列表中
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }

            // 如果所有的键都被遍历过，获取兄弟节点的UID
            long siblingUid = 0;
            if(kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }

            // 创建一个LeafSearchRangeRes对象，用于存储搜索结果
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    /**
     * 向 B+树 插入一个键值对，必要时分裂节点
     * @param uid
     * @param key
     * @return
     * @throws Exception
     */
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        // 设置保存点，便于回滚
        dataItem.before();
        try {
            success = insert(uid, key);

            // 如果插入失败，设置兄弟节点的UID，并返回结果
            if(!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }

            // 判断是否需要分裂节点
            if(needSplit()) {
                try {
                    // 分裂节点，并获取分裂后的结果
                    SplitRes r = split();
                    // 设置新节点的 UID和键
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            // 若没有 err 并且插入成功，提交数据项的修改
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                // 出现问题就回滚
                dataItem.unBefore();
            }
        }
    }

    /**
     * 在B+树的节点中插入一个键值对的方法
     * @param uid
     * @param key
     * @return
     */
    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw); // 获取当前节点的子节点数量
        int kth = 0;
        // 从第0个子节点开始，寻找合适位置
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        // 如果所有的键都被遍历过，并且存在兄弟节点，插入失败
        if(kth == noKeys && getRawSibling(raw) != 0) return false;

        //  如果节点是叶子节点
        if(getRawIfLeaf(raw)) {
            shiftRawKth(raw, kth);  // kth开始的所有键值后移一位
            setRawKthKey(raw, key, kth);    // 设置键
            setRawKthSon(raw, uid, kth);    // 设置值
            setRawNoKeys(raw, noKeys+1);    // 节点数量+1
        } else {// 非叶子节点
            long kk = getRawKthKey(raw, kth);   // 获取插入位置的键
            setRawKthKey(raw, key, kth);    // 在插入位置插入新的键
            shiftRawKth(raw, kth+1);    // 所有键和其子节点后移
            setRawKthKey(raw, kk, kth+1);   // 在新插入节点后的位置插入原来的节点和新的子节点 uid
            setRawKthSon(raw, uid, kth+1);
            setRawNoKeys(raw, noKeys+1);    //更新节点中的键的数量
        }
        return true;
    }

    /**
     * 当节点数大于平衡因子的两倍时，需要分裂几点
     * @return
     */
    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    /**
     * 分裂 B+树 节点
     * 当一个节点的键的数量达到 `BALANCE_NUMBER * 2` 时，就意味着这个节点已经满了，需要进行分裂操作。
     * 分裂操作的目的是将一个满的节点分裂成两个节点，每个节点包含一半的键。
     * @return
     * @throws Exception
     */
    private SplitRes split() throws Exception {
        // 创建新的节点，存储新的节点的原始数据
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        // 设置新节点的叶子标记，与原节点相同
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        // 设置新节点的节点数量为 Balance_Number，即满容量的一半
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        // 设置新节点的兄弟节点，与原节点一致
        setRawSibling(nodeRaw, getRawSibling(raw));
        // 从原节点的原始字节数组中复制一半数据到新节点的原始字节数组中
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        // 在数据管理器中插入新节点的原始数据，并获取新节点的UID
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);

        // 更新当前节点的键的数量
        setRawNoKeys(raw, BALANCE_NUMBER);
        // 将当前节点的兄弟节点的兄弟节点更新为 分裂的新节点的UID
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);  // 设置新节点的第一个键的值
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}
