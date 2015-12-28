package com.hiriver.unbiz.mysql.lib.protocol.tool;

import java.io.UnsupportedEncodingException;

public class StringTool {
    private StringTool() {
    }

    public static byte[] safeConvertString2Bytes(String str) {
        try {
            return str.getBytes("utf-8");

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String safeConvertBytes2String(byte[] buff) {
        try {
            return new String(buff, "utf-8");

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String safeConvertBytes2String(byte[] buff, String charsetName) {
        try {
            return new String(buff, charsetName);

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
