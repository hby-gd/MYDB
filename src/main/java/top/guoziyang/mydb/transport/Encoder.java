package top.guoziyang.mydb.transport;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.common.Error;

public class Encoder {

    /**
     * 将pkg编码为字节数组，通过首字节判断是err还是data
     * @param pkg
     * @return
     */
    public byte[] encode(Package pkg) {
        if(pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if(err.getMessage() != null) {
                msg = err.getMessage();
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    /**
     * 将 data 解析为 pkg
     * @param data
     * @return
     * @throws Exception
     */
    public Package decode(byte[] data) throws Exception {
        if(data.length < 1) {
            throw Error.InvalidPkgDataException;
        }
        if(data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if(data[0] == 1) {
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            throw Error.InvalidPkgDataException;
        }
    }

}
