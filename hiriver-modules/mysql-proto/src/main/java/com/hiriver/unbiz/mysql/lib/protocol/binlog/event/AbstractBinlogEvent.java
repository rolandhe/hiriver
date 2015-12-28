package com.hiriver.unbiz.mysql.lib.protocol.binlog.event;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.AbstractBinlogResponse;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;

public abstract class AbstractBinlogEvent extends AbstractBinlogResponse implements BinlogEvent {
    private final long eventBinlogPos;
    private long occurTime;
    private final boolean hasCheckSum;

    protected AbstractBinlogEvent(long eventBinlogPos, boolean hasCheckSum) {
        this.eventBinlogPos = eventBinlogPos;
        this.hasCheckSum = hasCheckSum;
    }

    public boolean isHasCheckSum() {
        return hasCheckSum;
    }

    @Override
    public long getBinlogEventPos() {
        return eventBinlogPos;
    }

    @Override
    public void acceptOccurTime(long occurTime) {
        this.occurTime = occurTime;
    }

    @Override
    public long getOccurTime() {
        return occurTime;
    }
}
