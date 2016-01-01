package com.hiriver.unbiz.mysql.lib.protocol.tool;

import java.io.UnsupportedEncodingException;

/**
 * 字符串处理工具
 * 
 * @author hexiufeng
 *
 */
public class StringTool {
    private StringTool() {
    }

    /**
     * 转换string为utf8 数组，不抛出异常
     * 
     * @param str 要转换的字符串
     * @return utf8数组
     */
    public static byte[] safeConvertString2Bytes(String str) {
        try {
            return str.getBytes("utf-8");

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 把指定的数组转换成string，数组是utf8编码的
     * 
     * @param buff utf8编码的数组
     * @return 字符串
     */
    public static String safeConvertBytes2String(byte[] buff) {
        try {
            return new String(buff, "utf-8");

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 按照指定的字符集转换byte数组为string
     * 
     * @param buff 指定字符集编码的数组
     * @param charsetName 字符集
     * @return 转换的字符串
     */
    public static String safeConvertBytes2String(byte[] buff, String charsetName) {
        try {
            return new String(buff, charsetName);

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
