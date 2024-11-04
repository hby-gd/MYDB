package top.guoziyang.mydb.backend.tbm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.parser.statement.Create;
import top.guoziyang.mydb.backend.parser.statement.Delete;
import top.guoziyang.mydb.backend.parser.statement.Insert;
import top.guoziyang.mydb.backend.parser.statement.Select;
import top.guoziyang.mydb.backend.parser.statement.Update;
import top.guoziyang.mydb.backend.parser.statement.Where;
import top.guoziyang.mydb.backend.tbm.Field.ParseValueRes;
import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.ParseStringRes;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

/**
 * Table 维护了表结构
 * 二进制结构如下：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 */
public class Table {
    TableManager tbm;   // 表管理器
    long uid;   // 当前表的唯一标识符
    String name;    // 表名字
    byte status;    // 表状态
    long nextUid;   // 下一个表的uid
    List<Field> fields = new ArrayList<>(); // 表的字段列表

    /**
     * 从磁盘中加载一个表
     *
     * @param tbm
     * @param uid
     * @return
     */
    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            // 读取表的二进制元数据
            raw = ((TableManagerImpl) tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        // 创建表对象
        Table tb = new Table(tbm, uid);
        // 从原始数据解析表对象并返回表对象
        return tb.parseSelf(raw);
    }

    /**
     * 创建表
     *
     * @param tbm     表管理器
     * @param nextUid 下一个表的uid
     * @param xid     事务ID
     * @param create  （表名，字段名，字段类型，索引）
     * @return 返回表对象实例
     * @throws Exception
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        // 创建基本表对象
        Table tb = new Table(tbm, create.tableName, nextUid);

        //遍历Create字段列表
        for (int i = 0; i < create.fieldName.length; i++) {
            // 获取字段及其类型
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];

            // 判断当前字段是否需要建立索引
            boolean indexed = false;
            for (int j = 0; j < create.index.length; j++) {
                if (fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            // 创建字段对象，加入字段列表中
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        // 持久化表对象元数据，并返回表对象
        return tb.persistSelf(xid);
    }

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /**
     * 从二进制元数据解析表内容
     *
     * @param raw
     * @return
     */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        // 解析 表名 及 相邻表uid
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        position += 8;

        // 解析并添加字段信息
        while (position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    /**
     * 将当前 表对象元数据 持久化到磁盘中
     * @param xid
     * @return 表对象
     * @throws Exception
     */
    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for (Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        // 更新当前表对象的uid
        uid = ((TableManagerImpl) tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    /**
     * 删除 Delete 条件的内容
     * @param xid   事务ID
     * @param delete    删除的条件
     * @return  删除的记录数
     * @throws Exception
     */
    public int delete(long xid, Delete delete) throws Exception {
        // 解析出符合删除条件的 节点 uid
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        for (Long uid : uids) {
            // 删除符合条件的记录
            if (((TableManagerImpl) tbm).vm.delete(xid, uid)) {
                count++;
            }
        }
        return count;
    }

    /**
     * 更新符合条件的记录
     * @param xid   事务id
     * @param update    更新的条件及内容
     * @return
     * @throws Exception
     */
    public int update(long xid, Update update) throws Exception {
        // 解析出符合条件的记录 uid
        List<Long> uids = parseWhere(update.where);
        Field fd = null;

        // 获取需要更新的字段对象
        for (Field f : fields) {
            if (f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        if (fd == null) {
            throw Error.FieldNotFoundException;
        }
        Object value = fd.string2Value(update.value);

        // 对所有符合条件的记录执行更新操作
        int count = 0;
        for (Long uid : uids) {
            // 读取记录的原始数据
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;

            // 删除旧数据
            ((TableManagerImpl) tbm).vm.delete(xid, uid);

            // 将旧记录的字节数组内容解析为 记录对象
            Map<String, Object> entry = parseEntry(raw);

            // 更新 update 中对应字段的值
            entry.put(fd.fieldName, value);

            // 将记录转化为 字节数组
            raw = entry2Raw(entry);

            // 插入记录
            long uuid = ((TableManagerImpl) tbm).vm.insert(xid, raw);

            count++;

            // 更新 B+树 索引信息
            for (Field field : fields) {
                if (field.isIndexed()) {
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }

    /**
     * 读取符合条件的信息
     * @param xid   事务id
     * @param read  查询要求
     * @return  返回查询结果的字符串
     * @throws Exception
     */
    public String read(long xid, Select read) throws Exception {
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            // 读取该索引的 字节数据
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;
            // 解析为一条记录
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }

    /**
     *
     * @param xid
     * @param insert
     * @throws Exception
     */
    public void insert(long xid, Insert insert) throws Exception {
        // 将新插入记录字符串转化为 entru 记录
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        // 写入磁盘
        long uid = ((TableManagerImpl) tbm).vm.insert(xid, raw);

        // 更新字段索引
        for (Field field : fields) {
            if (field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if (values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }

    /**
     * 从表中获取符合where条件的索引 叶子节点 uid 列表
     * @param where
     * @return
     * @throws Exception
     */
    private List<Long> parseWhere(Where where) throws Exception {
        long l0 = 0, r0 = 0, l1 = 0, r1 = 0;
        boolean single = false;
        Field fd = null;

        // 如果where条件为空，就寻找一个有索引的字段，从该索引所在的 B+树 获取数据
        if (where == null) {
            for (Field field : fields) {
                if (field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            // 获取第一个条件的对应索引的字段
            for (Field field : fields) {
                if (field.fieldName.equals(where.singleExp1.field)) {
                    if (!field.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            if (fd == null) {
                throw Error.FieldNotFoundException;
            }
            // 计算该字段的索引结果
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0;
            r0 = res.r0;
            l1 = res.l1;
            r1 = res.r1;
            single = res.single;
        }

        // 查找该字段索引上符合条件的 uid 信息
        List<Long> uids = fd.search(l0, r0);
        // 若有两个条件，则获取该字段符合第二个条件的 uid 信息
        if (!single) {
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }

    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    /**
     * 计算在该字段索引的 B+树 上，符合条件的 索引的 uid 范围
     * @param fd    字段对象
     * @param where     条件
     * @return
     * @throws Exception
     */
    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        // 根据逻辑符号做处理
        switch (where.logicOp) {
            // 单条件
            case "":
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                break;
            // 或
            case "or":
                res.single = false;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                break;
            // 与，计算两个条件范围的交集
            case "and":
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left;
                res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left;
                res.r1 = r.right;
                if (res.l1 > res.l0) res.l0 = res.l1;
                if (res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return res;
    }

    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if (i == fields.size() - 1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    /**
     * 将字节数组解析为 记录
     * @param raw
     * @return  字段及其值的记录 map
     */
    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        // 依次解析各个字段的内容
        for (Field field : fields) {
            ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    /**
     * 将记录中的各个字段及其值转换为一个统一的字节数组格式，通常用于存储
     * @param entry 记录对象
     * @return
     */
    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for (Field field : fields) {
            sb.append(field.toString());
            if (field == fields.get(fields.size() - 1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
