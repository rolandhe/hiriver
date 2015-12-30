package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.tool.GTSidTool;

/**
 * 描述一个gtid，format: uuid:transactionid
 * 
 * @author hexiufeng
 *
 */
public class GTIDInfo {
    private final String uuid;
    private long stop;

    /**
     * 构造器
     * 
     * @param gtidInfoString uuid:12
     */
    public GTIDInfo(String gtidInfoString) {
        String[] array = gtidInfoString.split(":");
        uuid = array[0];
        stop = Long.parseLong(array[1]);
    }

    public String getUuid() {
        return uuid;
    }

    public byte[] getUuidBytes() {
        return GTSidTool.convertSidString2DumpFormatBytes(uuid);
    }

    public long getStop() {
        return stop;
    }

    public void setStop(long stop) {
        this.stop = stop;
    }
    
    @Override
    public String toString(){
        return this.uuid + ":" + this.stop;
    }
}
