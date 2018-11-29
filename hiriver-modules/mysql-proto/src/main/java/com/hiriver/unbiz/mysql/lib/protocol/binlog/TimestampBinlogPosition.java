package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.Request;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * <pre>
 * 基于时间戳的位点，包含时间戳、mysql server uuid、binlog文件名称、文件内偏移量 4个字段；
 * 有效格式包含如下3种
 *     1543374986
 *     1543374986:::
 *     1543374686:a20181c1-7edb-11e6-8e14-90e2bac75fc4:mysql-bin.000178:515628788
 * 即或者只有时间戳或者有全部4个字段
 * </pre>
 * created by Yang Huawei (xander.yhw@alibaba-inc.com) on 2018/8/29 22:23
 */
public class TimestampBinlogPosition implements BinlogPosition {
    /**
     * unix时间戳，单位秒
     */
    private final long timestamp;
    private final String serverUuid;
    private final String binlogFileName;
    private final Long pos;


    public TimestampBinlogPosition(long timestamp) {
        this(timestamp, null, null, null);
    }

    public TimestampBinlogPosition(long timestamp, String serverUuid, String binlogFileName,
            Long pos) {
        this.timestamp = timestamp;
        this.serverUuid = serverUuid;
        this.binlogFileName = binlogFileName;
        this.pos = pos;
    }

    @Override
    public Request packetDumpRequest(int serverId) {
        return new BinlogFileBinlogPosition(this.binlogFileName, this.pos).packetDumpRequest(serverId);
    }

    @Override
    public byte[] toBytesArray() {
        return toString().getBytes();
    }

    @Override
    public String toString() {
        return timestamp + ":" + (serverUuid == null ? "" : serverUuid) + ":"
                + (binlogFileName == null ? "" : binlogFileName) + ":" + (pos == null ? "" : pos);
    }

    @Override
    public boolean isSame(BinlogPosition pos) {
        return this.equals(pos);
    }


    public long getTimestamp() {
        return timestamp;
    }

    public String getServerUuid() {
        return serverUuid;
    }

    public Long getPos() {
        return pos;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TimestampBinlogPosition that = (TimestampBinlogPosition) o;

        if (timestamp != that.timestamp)
            return false;
        if (serverUuid != null ? !serverUuid.equals(that.serverUuid) : that.serverUuid != null)
            return false;
        if (binlogFileName != null ? !binlogFileName.equals(that.binlogFileName)
                : that.binlogFileName != null)
            return false;
        return pos != null ? pos.equals(that.pos) : that.pos == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (serverUuid != null ? serverUuid.hashCode() : 0);
        result = 31 * result + (binlogFileName != null ? binlogFileName.hashCode() : 0);
        result = 31 * result + (pos != null ? pos.hashCode() : 0);
        return result;
    }

}
