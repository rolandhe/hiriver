package com.hiriver.unbiz.mysql.lib.protocol.datautils;

import java.util.Arrays;

import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriverunbiz.mysql.lib.exp.InvalidMysqlDataException;

/**
 * 处理mysql string类型的数据工具，mysql string本质上是byte[].
 * 
 * ses
 * <a href="http://dev.mysql.com/doc/internals/en/string.html"> http://dev.mysql.com/doc/internals/en/string.html </a>
 * 
 * @author hexiufeng
 *
 */
public class MysqlStringUtils {
    private MysqlStringUtils() {
    }

    public static byte[] readNulString(byte[] buf, Position off) {
        int end = -1;
        int start = off.getPos();
        for (int i = start; i < buf.length; i++) {
            if (buf[i] == 0) {
                end = i;
                break;
            }
        }
        if (end == -1) {
            throw new InvalidMysqlDataException("invalid null string");
        }
        off.forwardPos(end + 1 - start);
        return Arrays.copyOfRange(buf, start, end);
    }

    public static byte[] readFixString(byte[] buf, Position off, int len) {
        int start = off.getPos();
        off.forwardPos(len);
        return Arrays.copyOfRange(buf, start, start + len);
    }

    public static byte[] readEofString(byte[] buf, Position off) {
        return readEofString(buf, off, false);
    }

    public static byte[] readEofString(byte[] buf, Position off, boolean hasCheckSum) {
        int len = buf.length - off.getPos();
        if (hasCheckSum) {
            len -= 4;
        }
        return readFixString(buf, off, len);
    }
}
