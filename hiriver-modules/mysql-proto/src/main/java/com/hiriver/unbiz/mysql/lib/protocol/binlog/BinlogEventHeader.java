package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.Response;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

/**
 * 
 * binlogEvent header,只支持 binlog verson 4
 * 
 * @author hexiufeng
 *
 */
public class BinlogEventHeader extends AbstractBinlogResponse implements Response {
    private int timestamp; // 4 bytes
    private int eventType; // 1 bytes
    private int serverId; // 4 bytes mysql server id
    private int eventSize; // 4 bytes
    private int logPos; // 4 bytes for binlog version >1
    private int flags; // 2 bytes for binlog version >1

    public int getRestContentLen() {
        return this.eventSize - getHeaderSize();
    }

    public int getHeaderSize() {
        return 19;
    }

    @Override
    public void parse(byte[] buf, Position pos) {
        this.timestamp = MysqlNumberUtils.read4Int(buf, pos);
        this.eventType = MysqlNumberUtils.read1Int(buf, pos);
        this.serverId = MysqlNumberUtils.read4Int(buf, pos);
        this.eventSize = MysqlNumberUtils.read4Int(buf, pos);
        this.logPos = MysqlNumberUtils.read4Int(buf, pos);
        this.flags = MysqlNumberUtils.read2Int(buf, pos);
    }

    public int getTimestamp() {
        return timestamp;
    }

    public int getEventType() {
        return eventType;
    }

    public int getServerId() {
        return serverId;
    }

    public int getLogPos() {
        return logPos;
    }

    public int getFlags() {
        return flags;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public int getEventSize() {
        return eventSize;
    }

    public void setEventSize(int eventSize) {
        this.eventSize = eventSize;
    }

    public void setLogPos(int logPos) {
        this.logPos = logPos;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }
}
