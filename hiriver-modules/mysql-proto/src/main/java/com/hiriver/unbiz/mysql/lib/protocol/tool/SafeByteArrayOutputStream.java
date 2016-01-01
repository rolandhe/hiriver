package com.hiriver.unbiz.mysql.lib.protocol.tool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * 继承{@link ByteArrayOutputStream}，输出数据时不抛出异常。
 * 用于打包Request
 * 
 * @author hexiufeng
 *
 */
public class SafeByteArrayOutputStream extends ByteArrayOutputStream {
    public SafeByteArrayOutputStream(int size) {
        super(size);
    }

    /**
     * 输出byte数组
     * 
     * @param buff byte数组
     */
    public void safeWrite(byte[] buff) {
        try {
            super.write(buff);
        } catch (IOException e) {
            // ignore
        }
    }

    /**
     * 重新设置某个位置的值
     * 
     * @param pos 指定的位置
     * @param value byte数据值
     */
    public void setPosValue(int pos, byte value) {
        super.buf[pos] = value;
    }
}
