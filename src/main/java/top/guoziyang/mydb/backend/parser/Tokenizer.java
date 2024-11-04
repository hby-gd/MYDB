package top.guoziyang.mydb.backend.parser;

import top.guoziyang.mydb.common.Error;

public class Tokenizer {
    private byte[] stat;
    private int pos;
    private String currentToken;
    private boolean flushToken;
    private Exception err;

    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    public String peek() throws Exception {
        if(err != null) {
            throw err;
        }
        // 判断是否需要刷新当前标记
        if(flushToken) {
            String token = null;
            try {
                // 获取 token
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    /**
     * 将当前的标记设置为需要刷新，下次再调用peek会生成新的标记
     */
    public void pop() {
        flushToken = true;
    }

    public byte[] errStat() {
        byte[] res = new byte[stat.length+3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos);
        return res;
    }

    /**
     * 更新指针位置
     */
    private void popByte() {
        pos ++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }

    /**
     * 返回当前指针指向的字符
     * @return
     */
    private Byte peekByte() {
        if(pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    private String next() throws Exception {
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    /**
     * 返回下一个元状态
     * @return
     * @throws Exception
     */
    private String nextMetaState() throws Exception {

        // 跳过空白字符
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                return "";
            }
            if(!isBlank(b)) {
                break;
            }
            popByte();
        }
        byte b = peekByte();

        // 判断当前字符是否为符号，若位符号，返回该符号
        if(isSymbol(b)) {
            popByte();
            return new String(new byte[]{b});
        } else if(b == '"' || b == '\'') {
            return nextQuoteState();// 如果是引号，处理引号状态，返回引号内的内容
        } else if(isAlphaBeta(b) || isDigit(b)) {
            return nextTokenState();// 如果是字母或数字，则获取标记状态
        } else {
            err = Error.InvalidCommandException;
            throw err;
        }
    }

    /**
     *
     * @return 提取并返回一个由字母、数字或下划线组成的标记
     * @throws Exception
     */
    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();

            // 判断字符是否是有效标记字符
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                // 若为空白则移动指针，跳过该字符
                if(b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString();
            }
            // 记录并跳过当前字符
            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }

    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    /**
     * 处理被引号包围的字符串
     * @return
     * @throws Exception
     */
    private String nextQuoteState() throws Exception {
        // 跳过第一个引号
        byte quote = peekByte();
        popByte();

        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            // 匹配到第二个引号，退出循环
            if(b == quote) {
                popByte();
                break;
            }
            // 记录非引号的内容
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }

    /**
     * 判断当前字符是否为符号
     * @param b
     * @return
     */
    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
		b == ',' || b == '(' || b == ')');
    }

    /**
     * 判断当前字符是否为空格
     * @param b
     * @return
     */
    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
