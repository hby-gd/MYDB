package top.guoziyang.mydb.backend.dm.pageCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;

    /**
     * 创建页面缓存
     * @param file
     * @param fileChannel
     * @param maxResource
     */
    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    /**
     * 新建一个页面，并将 initData 作为内容，写入磁盘，并返回页面编号
     * @param initData
     * @return
     */
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);
        return pgno;
    }

    /**
     * 从缓存中获取指定页面标号的页面
     * @param pgno  页面编号
     * @return  指定页面的内容
     * @throws Exception
     */
    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);
    }


    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     * @param key
     * @return Page包装的指定页数据
     * @throws Exception
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buf.array(), this);
    }

    /**
     * 将修改过的缓存内容写入磁盘文件中
     * @param pg
     */
    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }

    /**
     * 将指定页面的缓存内容写入磁盘，并更新当前缓存引用计数状态
     * @param page
     */
    public void release(Page page) {
        release((long)page.getPageNumber());
    }

    public void flushPage(Page pg) {
        flush(pg);
    }

    /**
     * 将 page内容持久化至磁盘中
     * @param pg
     */
    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * 清除 maxPgno后页面的内容，并更新文件长度，更新页面数量
     * @param maxPgno
     */
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    private static long pageOffset(int pgno) {
        return (pgno-1) * PAGE_SIZE;
    }
    
}
