package com.hiriver.unbiz.mysql.lib.protocol.tool;

/**
 * 判断common packet类型的工具
 * 
 * @author hexiufeng
 *
 */
public class PacketTool {
    private PacketTool() {
    }

    /**
     * 读取到的二进制数据是否是OK包数据
     * 
     * @param buf byte数组
     * @return 是否是OK包
     */
    public static boolean isOkPackete(byte[] buf) {
        return (buf[0] & 0xff) == 0;
    }

    /**
     * 读取到的二进制数据是否是ERR包数据
     * 
     * @param buf byte数组
     * @return 是否是ERR包
     * 
     */
    public static boolean isErrPackete(byte[] buf) {
        return (buf[0] & 0xff) == 0xff;
    }

    /**
     * 读取到的二进制数据是否是EOF包数据
     * 
     * @param buf byte数组
     * @return 是否是EOF包
     * 
     */
    public static boolean isEofPackete(byte[] buf) {
        return buf.length < 9 && (buf[0] & 0xfe) == 0xfe;
    }

    /**
     * 读取到的二进制数据中从指定的位置算起是否是EOF包数据
     * 
     * @param buf byte数组
     * @param pos 开始位置
     * @return 是否是EOF包
     */
    public static boolean isEofPackete(byte[] buf, int pos) {
        return buf.length - pos < 9 && (buf[pos] & 0xfe) == 0xfe;
    }

    /**
     * 读取到的二进制数据中从指定的位置算起是否是切换权限验证包数据
     * 
     * @param buf byte数组
     * @return 是否是切换权限验证包
     */
    public static boolean isChangeAuthPackete(byte[] buf) {
        return (buf[0] & 0xff) == 0xfe;
    }

}
