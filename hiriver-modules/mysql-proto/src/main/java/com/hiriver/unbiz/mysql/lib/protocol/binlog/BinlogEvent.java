package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.Response;

public interface BinlogEvent extends Response {
    long getBinlogEventPos();
    void acceptOccurTime(long occurTime);
    long getOccurTime();
}
