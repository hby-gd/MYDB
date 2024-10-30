package top.guoziyang.mydb.backend.dm;

import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.dm.logger.Logger;
import top.guoziyang.mydb.backend.dm.page.PageOne;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    /**
     * 创建DM
     * @param path
     * @param mem
     * @param tm
     * @return
     */
    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    /**
     * 打开指定DM文件
     * @param path
     * @param mem
     * @param tm
     * @return
     */
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        // 若上次为不正常退出，根据 re_log和undo_log来回滚事务
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }

        // 读取每一页编号及空闲空间，存储到 页面索引中
        dm.fillPageIndex();

        // 更新管理页状态并持久化
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
