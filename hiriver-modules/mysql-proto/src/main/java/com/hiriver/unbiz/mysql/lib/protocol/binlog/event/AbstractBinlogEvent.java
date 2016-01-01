package com.hiriver.unbiz.mysql.lib.protocol.binlog.event;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.AbstractBinlogResponse;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;

/**
 * 抽象的binlog 事件描述
 * 
 * @author hexiufeng
 *
 */
public abstract class AbstractBinlogEvent extends AbstractBinlogResponse implements BinlogEvent {
    /**
     * 该事件在binlog file内部的位置
     */
    private final long eventBinlogPos;
    /**
     * 该数据写入mysql的事件戳，一般用于检测数据同步的性能
     */
    private long occurTime;
    /**
     * mysql日志是否支持校验
     */
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
