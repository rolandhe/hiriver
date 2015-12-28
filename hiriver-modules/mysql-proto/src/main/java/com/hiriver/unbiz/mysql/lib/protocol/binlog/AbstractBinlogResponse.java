package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.Response;

public abstract class AbstractBinlogResponse implements Response {

    @Override
    public void parse(byte[] buf) {
        throw new RuntimeException("don't support this method.");
    }
}
