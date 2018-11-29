package com.hiriver.channel.stream.impl;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.TimestampBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;

/**
 * created by Yang Huawei (xander.yhw@alibaba-inc.com) on 2018/8/29 23:01
 */
public class TimestampTransactionRecognizer extends AbstractTransactionRecognizer {

    private TimestampBinlogPosition position;

    @Override
    public boolean isStart(ValidBinlogOutput validOutput) {
        boolean start = super.isStart(validOutput);
        if (start) {
            position = new TimestampBinlogPosition(validOutput.getEvent().getOccurTime(),
                    validOutput.getServerUuid(), validOutput.getBinlogFileName(),
                    validOutput.getEvent().getBinlogEventPos());
            super.transBinlogPos = validOutput.getEventBinlogPos();
        }
        return start;
    }

    @Override
    public boolean isEnd(ValidBinlogOutput validOutput) {
        boolean isEnd = super.isEnd(validOutput);
        if (isEnd) {
            position = new TimestampBinlogPosition(validOutput.getEvent().getOccurTime(),
                    validOutput.getServerUuid(), validOutput.getBinlogFileName(),
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
    public TimestampBinlogPosition getCurrentTransBeginPos() {
        return position;
    }

}
