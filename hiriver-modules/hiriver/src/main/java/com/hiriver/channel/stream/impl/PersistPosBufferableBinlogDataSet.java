package com.hiriver.channel.stream.impl;

import com.hiriver.channel.BinlogDataSet;
import com.hiriver.channel.stream.BufferableBinlogDataSet;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

final class PersistPosBufferableBinlogDataSet implements BufferableBinlogDataSet {
    private final BinlogDataSet ds;
    private final BinlogPosition pos;
    PersistPosBufferableBinlogDataSet(BinlogDataSet ds,BinlogPosition pos){
        this.ds = ds;
        this.pos = pos;
    }
    @Override
    public BinlogDataSet getBinlogDataSet() {
        return ds;
    }
    public BinlogPosition getPos() {
        return pos;
    }

}
