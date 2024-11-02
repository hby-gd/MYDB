package top.guoziyang.mydb.backend.vm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {

    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    /**
     * 读信息
     * @param xid   事务ID
     * @param uid   Entry ID
     * @return  返回数据项内容
     * @throws Exception
     */
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        // 获取事务对象
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        // 事务是否有问题
        if(t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            // 从缓存中读取数据
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            // 数据对当前事务是否可见
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    /**
     * 事务 xid 插入数据，返回dm中数据项uid
     * @param xid
     * @param data
     * @return
     * @throws Exception
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        // 获取 事务对象
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        // 将原始数据包装为 Entry raw
        byte[] raw = Entry.wrapEntryRaw(xid, data);

        // 使用dm插入 Entry raw 并返回数据项 uid
        return dm.insert(xid, raw);
    }

    /**
     * 删除Entry 数据项
     * @param xid
     * @param uid
     * @return
     * @throws Exception
     */
    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try {
            // 从缓存中读 Entry对象
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            // 缓存是否对当前事务可见
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                // 对当前数据项加锁，并获取到该锁
                l = lt.add(xid, uid);
            } catch(Exception e) {
                t.err = Error.ConcurrentUpdateException;
                // 自动中断事务
                internAbort(xid, true);
                // 标记事务被自动中断
                t.autoAborted = true;
                throw t.err;
            }
            // 释放该锁
            if(l != null) {
                l.lock();
                l.unlock();
            }

            // 判断当前
            if(entry.getXmax() == xid) {
                return false;
            }

            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid);
            return true;

        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin(); // 开启事务，返回新事务ID
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction); // 创建事务对象
            activeTransaction.put(xid, t); // 向活跃事务Map中添加该事务
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if(t.err != null) {
                throw t.err;
            }
        } catch(NullPointerException n) {
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        // 从活跃事务中释放该事务
        activeTransaction.remove(xid);
        lock.unlock();

        // 从锁表中移除该事务的锁
        lt.remove(xid);

        // 提交事务
        tm.commit(xid);
    }

    /**
     * 手动中断事务
     * @param xid
     */
    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    /**
     * 中断事务
     * @param xid   事务ID
     * @param autoAborted   是否自动中断
     */
    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        // 手动中止时从活跃事务列表中移除该事务
        if(!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        // 若事务已被自动终止，不做处理
        if(t.autoAborted) return;
        // 从锁表中移除锁
        lt.remove(xid);
        // 终止事务
        tm.abort(xid);
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null) {
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
    
}
