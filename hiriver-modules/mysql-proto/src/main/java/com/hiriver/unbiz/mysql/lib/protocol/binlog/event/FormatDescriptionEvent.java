package com.hiriver.unbiz.mysql.lib.protocol.binlog.event;

import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

public class FormatDescriptionEvent extends AbstractBinlogEvent implements BinlogEvent {

    private int binlogVersion; // 2 bytes
    private String mysqlSeverVersion; // 50 bytes
    private long createStamp; // 4 bytes, unix stamp
    private int eventHeaderLen; // 1 bytes, should be 19
    private byte[] eventTypeHeaderLenArray; // string.eof

    public FormatDescriptionEvent(long eventBinlogPos, boolean hasCheckSum) {
        super(eventBinlogPos, hasCheckSum);
    }

    @Override
    public void parse(byte[] buf, Position pos) {
        this.binlogVersion = MysqlNumberUtils.read2Int(buf, pos);
        this.mysqlSeverVersion = new String(MysqlStringUtils.readFixString(buf, pos, 50));
        this.createStamp = MysqlNumberUtils.read4Int(buf, pos) & 0xffffffffL;
        this.eventHeaderLen = MysqlNumberUtils.read1Int(buf, pos);
        this.eventTypeHeaderLenArray = MysqlStringUtils.readEofString(buf, pos, super.isHasCheckSum());
    }

    public int getPostHeaderLen(int eventType) {
        return eventTypeHeaderLenArray[eventType - 1] & 0xff;
    }

    public int getBinlogVersion() {
        return binlogVersion;
    }

    public String getMysqlSeverVersion() {
        return mysqlSeverVersion;
    }

    public long getCreateStamp() {
        return createStamp;
    }

    public int getEventHeaderLen() {
        return eventHeaderLen;
    }

    public byte[] getEventTypeHeaderLenArray() {
        return eventTypeHeaderLenArray;
    }
}
