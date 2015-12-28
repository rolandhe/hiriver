package com.hiriver.unbiz.mysql.lib.protocol;

import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

/**
 * 与mysql进行交互的数据包的头，msyql的由固定4个字节组成：
 * <ul>
 * <li>前三个字节是payloadLen</li>
 * <li>最后一个字节是sequenceId</li>
 * </ul>
 * 
 * @author hexiufeng
 * 
 */
public class PacketHeader extends AbstractResponse implements Response, Request {
    /**
     * 后续数据的字节数
     */
    private int payloadLen;
    /**
     * 数据包的序号，从0开始，下一名开始时重置为0
     */
    private int sequenceId;

    public int getPayloadLen() {
        return payloadLen;
    }

    public void setPayloadLen(int payloadLen) {
        this.payloadLen = payloadLen;
    }

    public int getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(int sequenceId) {
        this.sequenceId = sequenceId;
    }

    @Override
    public void parse(byte[] packetHeader) {
        Position pos = Position.factory();
        payloadLen = MysqlNumberUtils.read3Int(packetHeader, pos);
        sequenceId = MysqlNumberUtils.read1Int(packetHeader, pos);
    }

    @Override
    public byte[] toByteArray() {
        byte[] buffer = MysqlNumberUtils.write4Int(payloadLen);
        buffer[3] = (byte) sequenceId;
        return buffer;
    }

    /**
     * header的字节数
     * 
     * @return header的字节数
     */
    public int getExpectLen() {
        return 4;
    }
}
