package com.hiriver.unbiz.mysql.lib.protocol.datautils;

import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriverunbiz.mysql.lib.exp.InvalidMysqlDataException;

/**
 * mysql 整形数据处理工具.mysql支持1,2,4,6,8个字节类型的整形数据。<br>
 * 
 * see <a href="http://dev.mysql.com/doc/internals/en/integer.html" > http://dev.mysql.com/doc/internals/en/integer.html
 * </a>
 * 
 * @author hexiufeng
 * 
 */
public class MysqlNumberUtils {
    private MysqlNumberUtils() {
    }

    public static int read1Int(byte[] buf, Position off) {
        return leftShiftByte2int(buf[off.getAndForwordPos()]);
    }

    public static int read2Int(byte[] buf, Position off) {
        int value = 0;
        value |= leftShiftByte2int(buf[off.getAndForwordPos()]);
        value |= leftShiftByte2int(buf[off.getAndForwordPos()], 8);
        return value;
    }

    public static int read3Int(byte[] buf, Position off) {
        int value = 0;
        value |= leftShiftByte2int(buf[off.getAndForwordPos()]);
        value |= leftShiftByte2int(buf[off.getAndForwordPos()], 8);
        value |= leftShiftByte2int(buf[off.getAndForwordPos()], 16);
        return value;
    }

    public static int read4Int(byte[] buf, Position off) {
        int value = 0;
        value |= leftShiftByte2int(buf[off.getAndForwordPos()]);
        value |= leftShiftByte2int(buf[off.getAndForwordPos()], 8);
        value |= leftShiftByte2int(buf[off.getAndForwordPos()], 16);
        value |= leftShiftByte2int(buf[off.getAndForwordPos()], 24);
        return value;
    }

    public static long read6Int(byte[] buf, Position off) {
        return readNInt(buf, off, 6);
    }

    public static long read8Int(byte[] buf, Position off) {
        return readNInt(buf, off, 8);
    }

    public static int readNRawInt(byte[] buf, Position off, int len) {
        int value = 0;
        for (int i = 0; i < len; i++) {
            if (i < len - 1) {
                value |= leftShiftByte2int(buf[off.getAndForwordPos()], i * 8);
            } else {
                value |= leftShiftByte2int(buf[off.getAndForwordPos()], i * 8);
            }
        }
        return value;
    }

    public static long readNInt(byte[] buf, Position off, int len) {
        long value = 0;
        for (int i = 0; i < len; i++) {
            value |= leftShiftByte2long(buf[off.getAndForwordPos()], i * 8);
        }
        return value;
    }

    public static long readBigEdianNInt(byte[] buf, Position off, int len) {
        long value = 0;
        for (int i = len - 1; i >= 0; i--) {
            value |= leftShiftByte2long(buf[off.getAndForwordPos()], i * 8);
        }
        return value;
    }

    public static long readLencodeLong(byte[] buf, Position off) {
        int flag = buf[off.getAndForwordPos()] & 0xff;
        if (flag < 0xfb) {
            return flag;
        }
        if (flag == 0xfc) {
            return (long) read2Int(buf, off);
        }
        if (flag == 0xfd) {
            return (long) read3Int(buf, off);
        }
        if (flag == 0xfe) {
            return read8Int(buf, off);
        }
        throw new InvalidMysqlDataException("invalid lencode");
    }

    public static byte[] write4Int(int value) {
        return writeNInt(value, 4);
    }

    public static byte[] writeNInt(int value, int len) {
        byte[] buf = new byte[len];
        for (int i = 0; i < len; i++) {
            buf[i] = (byte) (value >> (i * 8));
        }
        return buf;
    }

    public static byte[] writeNLong(long value, int len) {
        byte[] buf = new byte[len];
        for (int i = 0; i < len; i++) {
            buf[i] = (byte) (value >> (i * 8));
        }
        return buf;
    }

    public static byte[] wirteLencodeLong(long value) {
        if (value < 0xfbL) {
            return new byte[] { (byte) value };
        }
        if (value >= 0xfbL && value <= 0xffffL) {
            return new byte[] { (byte) 0xfc, (byte) value, (byte) (value >> 8) };
        }
        if (value > 0xffffL && value <= 0xffffffL) {
            return new byte[] { (byte) 0xfd, (byte) value, (byte) (value >> 8), (byte) (value >> 16) };
        }

        byte[] buffer = new byte[9];

        buffer[0] = (byte) 0xfe;
        for (int i = 0; i < 8; i++) {
            buffer[i + 1] = (byte) (value >> (i * 8));
        }
        return buffer;
    }

    public static int getLencodeLen(byte[] buf, Position off) {
        int flag = buf[off.getPos()] & 0xff;
        if (flag < 0xfb) {
            return 1;
        }
        if (flag == 0xfc) {
            return 3;
        }
        if (flag == 0xfd) {
            return 4;
        }
        if (flag == 0xfe) {
            return 9;
        }
        throw new InvalidMysqlDataException("invalid lencode");
    }

    public static byte[] writeBigEdianNInt(long value, int len) {
        byte[] buf = new byte[len];
        for (int i = len - 1; i >= 0; i--) {
            buf[i] = (byte) (value >>> (i * 8));
        }
        return buf;
    }

    public static boolean isValidLencodeLong(byte[] buf, Position off) {
        int flag = buf[off.getPos()] & 0xff;
        int restLen = buf.length - off.getPos();
        if (flag < 0xfb) {
            return true;
        }
        if (flag == 0xfc) {
            return true && restLen >= 2;
        }
        if (flag == 0xfd) {
            return true && restLen >= 3;
        }
        if (flag == 0xfe) {
            return true && restLen >= 8;
        }
        return false;
    }

    public static int leftShiftByte2int(byte byteValue) {
        return leftShiftByte2int(byteValue, 0);
    }

    public static int leftShiftByte2int(byte byteValue, int bitCount) {
        return (byteValue & 0xff) << bitCount;
    }

    public static long leftShiftByte2long(byte byteValue) {
        return leftShiftByte2int(byteValue, 0);
    }

    public static long leftShiftByte2long(byte byteValue, int bitCount) {
        return (byteValue & 0xffL) << bitCount;
    }
}
