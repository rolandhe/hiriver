package com.hiriver.unbiz.mysql.lib.protocol.binlog.event;

import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

/**
 * 日志翻滚事件，在执行dump指令后，后者开启新日志文件时，都会发送该事件，它可以记录后续事件所在的binlog file name，
 * 在非gtid支持的场景下，非常有用，可以用于记录事件所在的位置
 * 
 * @author hexiufeng
 *
 */
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
