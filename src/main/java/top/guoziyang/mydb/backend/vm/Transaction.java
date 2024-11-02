package top.guoziyang.mydb.backend.vm;

import java.util.HashMap;
import java.util.Map;

import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;

// vm对一个事务的抽象
public class Transaction {
    public long xid;
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    /**
     *  创建一个事务对象
     *
     * @param xid   事务ID
     * @param level 隔离级别(0:读已提交   1:可重复读)
     * @param active    活跃事务映射的引用
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

    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
