package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.tm.TransactionManager;

public class Visibility {

    /**
     * 判断是否发生版本跳跃问题
     * MVCC中，一个事务可能会在未看到某些中间版本的情况下直接修改数据的最新版本，从而导致逻辑错误。在事务并发执行时，如果没有适当的控制机制，事务可能会产生数据不一致的情况。
     * 例：
     *      1. 事务 Ti 读取 entry 的版本1， Tj 此时更新 entry 的值到版本2，并提交
     *      2. 事务 Ti 继续执行，看到的仍然是 版本1的内容，此时 Tj 已经提交，导致 Ti 在执行过程中用到过时内容
     *
     * 解决思路：检查最新版本的创建者对当前事务是否可见，如果当前事务要修改的数据被其他数据修改，且对当前事务不可见，就要求当前事务回滚。
     * 具体情况：
     *      1. 事务 Tj 的事务ID 大于 Ti 的事务ID，表示 Tj在时间上晚于 Ti，Ti应该回滚，避免版本跳跃。
     *      2. 事务 Tj 在 Ti的快照集合中，即 Tj 在时间上早于Ti，但Ti看不到Tj的修改，Ti应该回滚
     *
     * @param tm    事务管理器
     * @param t     事务对象
     * @param e     数据项对象
     * @return
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        // 获取删除entry的事务ID
        long xmax = e.getXmax();

        // 读已提交允许版本跳跃，即不会发生版本跳跃问题
        if(t.level == 0) {
            return false;
        } else {
            // 删除已提交 并且 （出现情况1 或 者出现情况2）
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    /**
     * 判断数据项 e 对当前事务 t 是否可见
     * @param tm 事务管理器
     * @param t  事务
     * @param e   数据项
     * @return
     */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 处理读已提交隔离级别下数据项的可见性
     * @param tm 事务管理器
     * @param t  事务
     * @param e   数据项
     * @return
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();

        // 当前事务是数据项的创建者且未被删除，则对当前事务可见
        if(xmin == xid && xmax == 0) return true;

        // 检查创建数据项的事务是否已提交
        if(tm.isCommitted(xmin)) {
            // 如果已提交且数据项没有被删除，返回数据项可见
            if(xmax == 0) return true;

            // 如果数据项已被删除，判断删除事务是否已提交。如果未提交，返回数据项可见
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 处理可重复读隔离级别下数据项的可见性
     * @param tm 事务管理器
     * @param t  事务
     * @param e   数据项
     * @return
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        // 如果当前事务是创建者且未被删除，返回 true
        if(xmin == xid && xmax == 0) return true;

        // 检查创建者事务是否已提交，且 xmin 在当前事务 xid 之前，并且创建者事务不在事务快照中
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {

            // 如果数据项没有被删除，返回 true
            if(xmax == 0) return true;
            // 如果数据项被其他事务删除
            if(xmax != xid) {
                // 删除事务未提交或在快照中，返回 true
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
