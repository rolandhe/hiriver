package com.hiriver.unbiz.mysql.lib.protocol.tool;

public class PacketTool {
    private PacketTool() {
    }

    public static boolean isOkPackete(byte[] buf) {
        return (buf[0] & 0xff) == 0;
    }

    public static boolean isErrPackete(byte[] buf) {
        return (buf[0] & 0xff) == 0xff;
    }

    public static boolean isEofPackete(byte[] buf) {
        return buf.length < 9 && (buf[0] & 0xfe) == 0xfe;
    }

    public static boolean isEofPackete(byte[] buf, int pos) {
        return buf.length - pos < 9 && (buf[pos] & 0xfe) == 0xfe;
    }

    public static boolean isChangeAuthPackete(byte[] buf) {
        return (buf[0] & 0xff) == 0xfe;
    }

}
