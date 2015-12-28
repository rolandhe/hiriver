package com.hiriver.unbiz.mysql.lib.protocol.binlog.extra;

import com.hiriver.unbiz.mysql.lib.protocol.Request;

public interface BinlogPosition {
    Request packetDumpRequest(int serverId);

    byte[] toBytesArray();
    boolean isSame(BinlogPosition posStore);
}
