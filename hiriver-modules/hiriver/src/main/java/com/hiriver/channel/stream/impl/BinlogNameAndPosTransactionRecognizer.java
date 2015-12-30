package com.hiriver.channel.stream.impl;

import com.hiriver.channel.stream.TransactionRecognizer;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogFileBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

public class BinlogNameAndPosTransactionRecognizer extends AbstractTransactionRecognizer
        implements TransactionRecognizer {
    private BinlogFileBinlogPosition position;

    @Override
    public boolean isStart(ValidBinlogOutput validOutput) {
        boolean start = super.isEnd(validOutput);
        if (start) {
            position = new BinlogFileBinlogPosition(validOutput.getBinlogFileName(),
                    validOutput.getEvent().getBinlogEventPos());
        }
        return start;
    }

    @Override
    public boolean isEnd(ValidBinlogOutput validOutput) {
        boolean isEnd = super.isEnd(validOutput);
        if (isEnd) {
            position = new BinlogFileBinlogPosition(validOutput.getBinlogFileName(),
                    validOutput.getEvent().getBinlogEventPos());
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

//    @Override
//    public BinlogPosition getCouldNextPos() {
//        return position;
//    }

}
