package com.hiriver.channel.stream;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

public interface TransactionRecognizer {
    boolean isStart(ValidBinlogOutput validOutput);
    boolean isEnd(ValidBinlogOutput validOutput);
    boolean tryRecognizePos(ValidBinlogOutput validOutput);
    BinlogPosition getCurrentTransBeginPos();
    BinlogPosition getCouldNextPos();
    String getGTId();
}
