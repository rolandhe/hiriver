package com.hiriver.unbiz.mysql.lib.protocol;

import java.io.UnsupportedEncodingException;

import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

/**
 * 错误数据包。<br>
 * 
 * see
 * <a href="http://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html" > http://dev.mysql.com/doc/internals/en/packet
 * -ERR_Packet.html </a>
 * 
 * @author hexiufeng
 *
 */
public class ERRPacket extends AbstractResponse implements Response {
    /**
     * MUST be 0xff
     */
    private int header;
    private int errorCode;
    private int sqlStateMarker;
    private byte[] sqlState;
    private String errorMessage;

    @Override
    public void parse(byte[] buf) {
        Position pos = Position.factory();
        header = MysqlNumberUtils.read1Int(buf, pos);
        errorCode = MysqlNumberUtils.read2Int(buf, pos);
        sqlStateMarker = MysqlNumberUtils.read1Int(buf, pos);
        sqlState = MysqlStringUtils.readFixString(buf, pos, 5);
        try {
            errorMessage = new String(MysqlStringUtils.readEofString(buf, pos, super.isCheckSum()), "utf-8");
        } catch (UnsupportedEncodingException e) {
            // ignore
        }
    }

    public int getHeader() {
        return header;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public int getSqlStateMarker() {
        return sqlStateMarker;
    }

    public byte[] getSqlState() {
        return sqlState;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

}
