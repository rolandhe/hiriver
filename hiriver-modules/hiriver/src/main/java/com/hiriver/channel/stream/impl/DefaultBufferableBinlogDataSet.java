package com.hiriver.channel.stream.impl;

import com.hiriver.channel.BinlogDataSet;
import com.hiriver.channel.stream.BufferableBinlogDataSet;

final class DefaultBufferableBinlogDataSet implements BufferableBinlogDataSet {
    private final BinlogDataSet ds;
    DefaultBufferableBinlogDataSet(BinlogDataSet ds){
        this.ds = ds;
    }
    @Override
    public BinlogDataSet getBinlogDataSet() {
        return ds;
    }

}
