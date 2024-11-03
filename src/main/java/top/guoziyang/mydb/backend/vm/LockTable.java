package top.guoziyang.mydb.backend.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {
    
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }



    /**
     *  尝试为一个给定的事务 (xid) 获取一个资源 (uid) 的锁
     * @param xid 事务ID
     * @param uid entry uid
     * @return  不需要等待则返回null，否则返回锁对象
     * @throws Exception 会造成死锁则抛出异常
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // 判断事务是否拥有该资源，已拥有不需要获取其对应的锁
            if(isInList(x2u, xid, uid)) {
                return null;
            }

            // 资源没有被其他事务持有
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);                // 记录 事务 对 资源的持有关系
                putIntoList(x2u, xid, uid);      // 新增事务对资源的持有关系
                return null;
            }

            // 如果资源已经被其他事务持有，将当前事务添加到等待列表中
            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);    // 将事务的ID添加到资源的等待列表

            // 检查是否存在死锁
            if(hasDeadLock()) {
                waitU.remove(xid);  // 将事务从等待队列中移除
                removeFromList(wait, uid, xid); // 将 事务xid 从 资源 uid 的等待队列中移除
                throw Error.DeadlockException;  //抛出死锁异常
            }

            // 为当前事务加锁
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);   // 将锁加入等待锁列表
            return l;

        } finally {
            lock.unlock();
        }
    }

    /**
     * 从等待图中移除 事务xid的锁
     * @param xid
     */
    public void remove(long xid) {
        lock.lock();
        try {
            // 获取该事务已持有的资源ID列表
            List<Long> l = x2u.get(xid);
            if(l != null) {
                // 逐个释放该事务持有的资源
                while(l.size() > 0) {
                    Long uid = l.remove(0);  // 获取并移出列表中的第一个资源ID

                    selectNewXID(uid);  // 从当前资源的请求事务表中选择一个事务持有该资源
                }
            }

            waitU.remove(xid);      //从等待列表中移除事务 xid，表示该事务不再等待任何资源

            x2u.remove(xid);        // 从持有资源的映射关系中移除该事务，表示该事务不再持有任何资源

            waitLock.remove(xid);   // 从等待锁表中移除该事务的锁记录，表示该事务的所有锁已释放

        } finally {
            lock.unlock();
        }
    }

    /**
     * 从 uid 的请求等待表中选择一个事务持有该资源
     * @param uid
     */
    private void selectNewXID(long uid) {
        u2x.remove(uid);    // 作用是将资源 uid 从 u2x 映射中移除，表示不再有事务持有该资源

        List<Long> l = wait.get(uid);   // 获取等待该资源的事务ID列表

        if(l == null) return;   // 如果没有任何事务请求该资源，方法直接返回

        assert l.size() > 0;    // 确保资源的请求列表中至少有一个事务在等待该资源

        while(l.size() > 0) {
            long xid = l.remove(0); // 移除并获取等待队列第一的事务

            // 检查事务是否仍在等待锁，即判断该事务是否在空闲中
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {// 若该事务空闲
                u2x.put(uid, xid);  // 将该事务ID与资源ID建立新的持有关系
                Lock lo = waitLock.remove(xid); // 从等待锁中移除该事务，并获取该事务的锁
                waitU.remove(xid);  // 将事务从等待队列移除，表示该事务不再等待任何资源
                lo.unlock();    // 释放该事务的锁，表示该事务现在可以使用这个资源
                break;
            }
        }

        // 若等待队列为空，清理空等待列表
        if(l.size() == 0) wait.remove(uid);
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    /**
     * 判断当前是否存在死锁
     * @return
     */
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();     // 保存事务的访问记录
        stamp = 1;      // 初始化记录编号
        for(long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            // 判断当前事务是否已经被访问
            if(s != null && s > 0) {
                continue;
            }

            stamp ++;       // 更新访问记录

            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        // 递归边界
        Integer stp = xidStamp.get(xid);

        // 当前事务的记录存在且等于当前记录，表示存在循环依赖
        if(stp != null && stp == stamp) {
            return true;
        }

        // 当前事务在之前的访问中记录过，不存在循环依赖
        if(stp != null && stp < stamp) {
            return false;
        }


        xidStamp.put(xid, stamp);   // 更新 xid 的访问记录

        Long uid = waitU.get(xid);  // 获取当前事务等待资源ID

        if(uid == null) return false; //没有其他事务等待，不存在循环依赖

        Long x = u2x.get(uid);  // 获取资源的当前持有者

        assert x != null;   // 断言资源持有者存在

        return dfs(x);  // 递归检查资源持有者是否存在死锁
    }

    /**
     * 从 uid0 的等待列表中移除 uid1
     * @param listMap
     * @param uid0
     * @param uid1
     */
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    /**
     * 将 uid0 的等待列表中加入 uid1
     * @param listMap
     * @param uid0
     * @param uid1
     */
    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    /**
     * 判断 uid0 的等待队列中是否包含 uid1
     * @param listMap   资源与其等待队列的映射
     * @param uid0      被请求资源ID
     * @param uid1      请求者ID
     * @return
     */
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }

}
