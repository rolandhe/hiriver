package com.hiriver.unbiz.mysql.lib.protocol;

import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

/**
 * 正确响应数据包。<br>
 * 
 * see
 * <a href="http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html"> http://dev.mysql.com/doc/internals/en/packet-
 * OK_Packet.html </a>
 * 
 * @author hexiufeng
 *
 */
public class OKPacket extends AbstractResponse implements Response {
    /**
     * MUST be 0x00
     */
    private int header;
    private long affectedRows;
    private long lastInsertId;
    private int statusFlags;
    private int warnings;

    @Override
    public void parse(byte[] buf) {
        Position pos = Position.factory();
        header = MysqlNumberUtils.read1Int(buf, pos);
        affectedRows = MysqlNumberUtils.readLencodeLong(buf, pos);
        lastInsertId = MysqlNumberUtils.readLencodeLong(buf, pos);
        statusFlags = MysqlNumberUtils.read2Int(buf, pos);
        warnings = MysqlNumberUtils.read2Int(buf, pos);
    }

    public int getHeader() {
        return header;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public long getLastInsertId() {
        return lastInsertId;
    }

    public int getStatusFlags() {
        return statusFlags;
    }

    public int getWarnings() {
        return warnings;
    }

    public void setHeader(int header) {
        this.header = header;
    }

    public void setAffectedRows(long affectedRows) {
        this.affectedRows = affectedRows;
    }

    public void setLastInsertId(long lastInsertId) {
        this.lastInsertId = lastInsertId;
    }

    public void setStatusFlags(int statusFlags) {
        this.statusFlags = statusFlags;
    }

    public void setWarnings(int warnings) {
        this.warnings = warnings;
    }

}
