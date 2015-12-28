package com.hiriver.position.store;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

public interface BinlogPositionStore {
    void store(BinlogPosition binlogPosition, String channelId);

    BinlogPosition load(String channelId);
}
