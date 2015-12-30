package com.hiriver.channel.stream.impl;

import com.hiriver.channel.stream.TransactionRecognizer;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.GTidBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidEventType;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.GTidEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

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
        if(gtIdString == null){
            return null;
        }
        return new GTidBinlogPosition(gtIdString);
    }

    @Override
    public String getGTId() {
        return gtIdString;
    }

    

//    @Override
//    public BinlogPosition getCouldNextPos() {
//        GTIDInfo info = new GTIDInfo(gtIdString);
//        info.setStop(info.getStop() + 1);
//        GTidBinlogPosition pos = new GTidBinlogPosition(info.toString());
//        return pos;
//    }

    private String getNewGtId(ValidBinlogOutput validOutput) {
        if (validOutput.getEventType() == ValidEventType.GTID) {
            GTidEvent gtEvent = (GTidEvent) (validOutput.getEvent());
            return gtEvent.getGTidString();
        }
        return null;
    }
    

}
