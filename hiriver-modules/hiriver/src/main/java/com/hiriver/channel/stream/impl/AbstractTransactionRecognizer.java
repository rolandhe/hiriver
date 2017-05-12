package com.hiriver.channel.stream.impl;

import com.hiriver.channel.stream.TransactionRecognizer;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidEventType;

/**
 * {@link TransactionRecognizer} 的抽象实现
 * 
 * @author hexiufeng
 *
 */
public abstract class AbstractTransactionRecognizer implements TransactionRecognizer {
    protected String transBinlogPos;
    @Override
    public boolean isStart(ValidBinlogOutput validOutput) {
        if (validOutput.getEventType() == ValidEventType.TRAN_BEGIN) {
            return true;
        }
        return false;
    }

    @Override
    public boolean isEnd(ValidBinlogOutput validOutput) {
        if (validOutput.getEventType() == ValidEventType.TRANS_COMMIT) {
            return true;
        }
        return false;
    }

    @Override
    public String getGTId() {
        return null;
    }

    @Override
    public String getTransBinlogPos() {
        return transBinlogPos;
    }

}
