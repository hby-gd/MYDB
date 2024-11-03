package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.tm.TransactionManager;

public class Visibility {

    /**
     * 判断数据项版本是否被跳过
     * @param tm    事务管理器
     * @param t     事务对象
     * @param e     数据项对象
     * @return
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        // 获取删除entry的事务ID
        long xmax = e.getXmax();

        // 读已提交不允许版本跳跃
        if(t.level == 0) {
            return false;
        } else {
            // 当前事务在删除事务之前启动，因此当前事务应回滚以避免版本跳跃
            // 删除者是活跃的
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
