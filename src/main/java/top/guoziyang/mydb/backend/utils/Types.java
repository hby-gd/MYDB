package top.guoziyang.mydb.backend.utils;

public class Types {
    /**
     * 结合页码及偏移量 获取 uid
     * @param pgno
     * @param offset
     * @return
     */
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long)pgno;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}
