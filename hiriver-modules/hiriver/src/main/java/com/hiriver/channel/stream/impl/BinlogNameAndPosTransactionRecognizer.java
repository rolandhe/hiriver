package com.hiriver.channel.stream.impl;

import com.hiriver.channel.stream.TransactionRecognizer;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogFileBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * binlog file + pos方式的事务识别器实现
 * 
 * @author hexiufeng
 *
 */
public class BinlogNameAndPosTransactionRecognizer extends AbstractTransactionRecognizer
        implements TransactionRecognizer {
    private BinlogFileBinlogPosition position;

    @Override
    public boolean isStart(ValidBinlogOutput validOutput) {
        boolean start = super.isStart(validOutput);
        if (start) {
            position = new BinlogFileBinlogPosition(validOutput.getBinlogFileName(),
                    validOutput.getEvent().getBinlogEventPos());
            super.transBinlogPos = validOutput.getEventBinlogPos();
        }
        return start;
    }

    @Override
    public boolean isEnd(ValidBinlogOutput validOutput) {
        boolean isEnd = super.isEnd(validOutput);
        if (isEnd) {
            position = new BinlogFileBinlogPosition(validOutput.getBinlogFileName(),
                    validOutput.getEvent().getBinlogEventPos());
            super.transBinlogPos = validOutput.getEventBinlogPos();
        }
        return isEnd;
    }

    @Override
    public boolean tryRecognizePos(ValidBinlogOutput validOutput) {
        return false;
    }

    @Override
    public BinlogPosition getCurrentTransBeginPos() {
        return position;
    }
}
