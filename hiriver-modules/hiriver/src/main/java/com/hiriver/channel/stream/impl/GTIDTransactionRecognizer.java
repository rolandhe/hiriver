package com.hiriver.channel.stream.impl;

import com.hiriver.channel.stream.TransactionRecognizer;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.GTidBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidEventType;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.GTidEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * gtid方式的事务识别器。gtid方式下在事务开启后的第一事件就是GTID事件，同时GTID的不同标示者着上一个事务的结束。
 * 但我们并不使用该特性来判断事务是否结束
 * 
 * @author hexiufeng
 *
 */
public class GTIDTransactionRecognizer extends AbstractTransactionRecognizer implements TransactionRecognizer {
    private String gtIdString;

    @Override
    public boolean tryRecognizePos(ValidBinlogOutput validOutput) {
        String newGtId = getNewGtId(validOutput);
        if (newGtId != null) {
            gtIdString = newGtId;
            return true;
        }
        return false;
    }

    @Override
    public BinlogPosition getCurrentTransBeginPos() {
        if (gtIdString == null) {
            return null;
        }
        return new GTidBinlogPosition(gtIdString);
    }

    @Override
    public String getGTId() {
        return gtIdString;
    }

    private String getNewGtId(ValidBinlogOutput validOutput) {
        if (validOutput.getEventType() == ValidEventType.GTID) {
            GTidEvent gtEvent = (GTidEvent) (validOutput.getEvent());
            return gtEvent.getGTidString();
        }
        return null;
    }

}
