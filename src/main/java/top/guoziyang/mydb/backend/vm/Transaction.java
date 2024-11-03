package top.guoziyang.mydb.backend.vm;

import java.util.HashMap;
import java.util.Map;

import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;

// vm对一个事务的抽象
public class Transaction {
    public long xid; // 事务 id
    public int level; // 事务的隔离级别 0：读已提交 1：可重复读
    public Map<Long, Boolean> snapshot; // 快照映射，存储活跃的事务ID，可重复读隔离级别下，事务需要知道在其快照时间点之后有哪些事务是有效的
    public Exception err;   // 保存事务执行过程中的错误信息
    public boolean autoAborted; // 标记事务是否被自动中止

    /**
     *  创建一个事务对象
     *
     * @param xid   事务ID
     * @param level 隔离级别(0:读已提交   1:可重复读)
     * @param active    活跃事务映射的引用，通常是一个包含当前所有活跃事务的映射
     * @return
     */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0) {
            // 创建快照映射
            t.snapshot = new HashMap<>();
            // 将当前活跃事务加入快照映射中
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    /**
     * 判断事务是否在快照中，在快照中的事务为当前活跃的事务，其对 entry 的修改对其他事务不可见
     * @param xid
     * @return
     */
    public boolean isInSnapshot(long xid) {
        // 超级事务 0，对所有事务不可见
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
