package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.Request;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * binlog file name + offset的实现，一般用于mysql5.6.9之前的版本，之后的版本请
 * 使用{@link GTidBinlogPosition}
 * 
 * @author hexiufeng
 *
 */
public class BinlogFileBinlogPosition implements BinlogPosition {
    private long pos;
    private String binlogFileName;

    public BinlogFileBinlogPosition() {

    }

    public BinlogFileBinlogPosition(String binlogFileName, long pos) {
        this.pos = pos;
        this.binlogFileName = binlogFileName;
    }

    @Override
    public Request packetDumpRequest(int serverId) {
        DumpRequest request = new DumpRequest(pos, serverId, binlogFileName);
        return request;
    }

    @Override
    public byte[] toBytesArray() {
        return toString().getBytes();
    }

    @Override
    public String toString() {
        return binlogFileName + ":" + pos;
    }

    @Override
    public boolean isSame(BinlogPosition posStore) {
        if (!(posStore instanceof BinlogFileBinlogPosition)) {
            return false;
        }
        return toString().equals(posStore.toString());
    }

}
