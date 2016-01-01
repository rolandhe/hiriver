package com.hiriver.unbiz.mysql.lib.protocol.binlog.event;

import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

/**
 * Xid event,标志着事务的结束
 * 
 * @author hexiufeng
 *
 */
public class XidEvent extends AbstractBinlogEvent implements BinlogEvent {
    private long xid;

    public XidEvent(long eventBinlogPos, boolean hasCheckSum) {
        super(eventBinlogPos, hasCheckSum);
    }

    @Override
    public void parse(byte[] buf, Position pos) {
        this.xid = MysqlNumberUtils.read8Int(buf, pos);
    }

    public long getXid() {
        return xid;
    }

}
