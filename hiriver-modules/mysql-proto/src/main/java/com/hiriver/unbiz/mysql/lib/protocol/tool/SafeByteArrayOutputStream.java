package com.hiriver.unbiz.mysql.lib.protocol.tool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SafeByteArrayOutputStream extends ByteArrayOutputStream {
    public SafeByteArrayOutputStream(int size) {
        super(size);
    }

    public void safeWrite(byte[] buff) {
        try {
            super.write(buff);
        } catch (IOException e) {
            // ignore
        }
    }

    public void setPosValue(int pos, byte value) {
        super.buf[pos] = value;
    }
}
