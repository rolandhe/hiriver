package com.hiriver.unbiz.mysql.lib.protocol;

import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.SafeByteArrayOutputStream;

/**
 * mysql request的抽象实现
 * 
 * @author hexiufeng
 *
 */
public abstract class AbstractRequest implements Request {
    private int sequenceId;

    public int getSequenceId() {
        return sequenceId;
    }

    public void setSequenceId(int sequenceId) {
        this.sequenceId = sequenceId;
    }

    @Override
    public byte[] toByteArray() {
        SafeByteArrayOutputStream out = new SafeByteArrayOutputStream(256);
        // packet header holder
        out.safeWrite(MysqlNumberUtils.write4Int(0));
        fillPayload(out);
        byte[] buffer = out.toByteArray();
        PacketHeader header = new PacketHeader();
        header.setPayloadLen(buffer.length - 4);
        header.setSequenceId(sequenceId);

        System.arraycopy(header.toByteArray(), 0, buffer, 0, 4);
        return buffer;
    }

    /**
     * 填充请求主体对象
     * 
     * @param out 输出的byte[] 流
     */
    protected abstract void fillPayload(SafeByteArrayOutputStream out);
}
