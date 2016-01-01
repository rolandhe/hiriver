package com.hiriver.channel.stream.impl;

import com.hiriver.channel.BinlogDataSet;
import com.hiriver.channel.stream.BufferableBinlogDataSet;

/**
 * 封装具体的来自Binlog Row event数据的实现
 * 
 * @author hexiufeng
 *
 */
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
