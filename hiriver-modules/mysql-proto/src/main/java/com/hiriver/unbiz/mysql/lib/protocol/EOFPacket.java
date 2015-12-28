package com.hiriver.unbiz.mysql.lib.protocol;

import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

/**
 * 响应结束包。用于一个请求可能返回大量数据时，比执行一个sql查询.<br>
 * 
 * see
 * <a href="http://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html" > http://dev.mysql.com/doc/internals/en/packet
 * -EOF_Packet.html </a>
 * 
 * @author hexiufeng
 *
 */
public class EOFPacket extends AbstractResponse implements Response {
    /**
     * MUST be 0xfe
     */
    private int header;
    private int warnings;
    private int statusFlags;

    @Override
    public void parse(byte[] buf) {
        Position pos = Position.factory();
        header = MysqlNumberUtils.read1Int(buf, pos);
        warnings = MysqlNumberUtils.read2Int(buf, pos);
        statusFlags = MysqlNumberUtils.read2Int(buf, pos);
    }

    public int getHeader() {
        return header;
    }

    public int getWarnings() {
        return warnings;
    }

    public int getStatusFlags() {
        return statusFlags;
    }

    public void setHeader(int header) {
        this.header = header;
    }

    public void setWarnings(int warnings) {
        this.warnings = warnings;
    }

    public void setStatusFlags(int statusFlags) {
        this.statusFlags = statusFlags;
    }

}
