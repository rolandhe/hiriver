package com.hiriver.unbiz.mysql.lib.protocol.tool;

/**
 * GTID工具，用于把byte数组转换成mysql uuid strig，或者把
 * uuid string转换成byte数据
 * 
 * @author hexiufeng
 *
 */
public class GTSidTool {
    private GTSidTool() {

    }

    private final static char[] HEX_DICT =
            { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    /**
     * 把byte数组转换成mysql的uuid
     * 
     * @param sid byte数组
     * @return uuid string
     */
    public static String convertSid2UUIDString(byte[] sid) {
        StringBuilder sb = new StringBuilder(36);
        for (int i = 0; i < 16; i++) {
            if (i == 4 || i == 6 || i == 8 || i == 10) {
                sb.append('-');
            }
            int value = sid[i] & 0xff;
            sb.append(HEX_DICT[value >> 4]);
            sb.append(HEX_DICT[value & 0xf]);
        }
        return sb.toString();
    }

    /**
     * 用于把mysql uuid转换成byte数组
     * 
     * @param uuidString uuid string
     * @return byte数组
     */
    public static byte[] convertSidString2DumpFormatBytes(String uuidString) {
        String value = uuidString.replaceAll("-", "");
        byte[] buffer = new byte[16];

        for (int i = 0; i < 16; i++) {
            String hex = value.substring(2 * i, 2 * i + 2);
            int intValue = Integer.parseInt(hex, 16);
            buffer[i] = (byte) intValue;
        }
        return buffer;
    }
}
