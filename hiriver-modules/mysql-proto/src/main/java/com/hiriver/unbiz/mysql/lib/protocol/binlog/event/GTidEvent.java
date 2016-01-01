package com.hiriver.unbiz.mysql.lib.protocol.binlog.event;

import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.GTSidTool;

/**
 * gtid事件，在mysql5.6.9之后添加的事件，描述该事务的唯一id，在一个事务中，第一个被发送的事件
 * 
 * @author hexiufeng
 *
 */
public class GTidEvent extends AbstractBinlogEvent implements BinlogEvent {
    private boolean commitFlag;
    private byte[] sid;
    private long gno;

    public GTidEvent(long eventBinlogPos, boolean hasCheckSum) {
        super(eventBinlogPos, hasCheckSum);
    }

    @Override
    public void parse(byte[] buf, Position pos) {
        int commitValue = MysqlNumberUtils.read1Int(buf, pos);
        commitFlag = commitValue == 1;
        sid = MysqlStringUtils.readFixString(buf, pos, 16);
        gno = MysqlNumberUtils.read8Int(buf, pos);
    }

    public String getGTidString() {
        return GTSidTool.convertSid2UUIDString(sid) + ":" + gno;
    }

    public boolean isCommitFlag() {
        return commitFlag;
    }

    public byte[] getSid() {
        return sid;
    }

    public String getSidString() {
        return GTSidTool.convertSid2UUIDString(sid);
    }

    public long getGno() {
        return gno;
    }
}
