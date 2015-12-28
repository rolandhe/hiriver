package com.hiriver.unbiz.mysql.lib.protocol.binlog.event;

import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

public class RotateEvent extends AbstractBinlogEvent implements BinlogEvent {
    private long position;
    private String nextBinlogName;

    public RotateEvent(long eventBinlogPos, boolean hasCheckSum) {
        super(eventBinlogPos, hasCheckSum);
    }

    @Override
    public void parse(byte[] buf, Position pos) {
        this.position = MysqlNumberUtils.read8Int(buf, pos);
        this.nextBinlogName = new String(MysqlStringUtils.readEofString(buf, pos, super.isHasCheckSum()));
    }

    public long getPosition() {
        return position;
    }

    public String getNextBinlogName() {
        return nextBinlogName;
    }

}
