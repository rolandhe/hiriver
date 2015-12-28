package com.hiriver.unbiz.msyql;

import org.junit.Test;

import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

public class TestNumberUtils {
    @Test
    public void test3() {
        byte[] bin = "123".getBytes();
        byte[] buf = { (byte) 0xfe, 0x29 };
        Position off = Position.factory();
        int value = (int) MysqlNumberUtils.readNInt(buf, off, 2);
    }
}
