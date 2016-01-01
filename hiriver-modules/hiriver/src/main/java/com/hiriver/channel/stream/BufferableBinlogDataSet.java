package com.hiriver.channel.stream;

import com.hiriver.channel.BinlogDataSet;

/**
 * 内部使用的接口，描述可以被发送到Queue缓冲的{@link com.hiriver.channel.BinlogDataSet}数据，
 * 用于封装{@link com.hiriver.channel.BinlogDataSet}，主要用于区分具体的binlog数据和事务结束
 * 信号数据。
 * 
 * @author hexiufeng
 * @see {@link com.hiriver.channel.BinlogDataSet}
 *
 */
public interface BufferableBinlogDataSet {
    /**
     * 获取 BinlogDataSet数据
     * 
     * @return BinlogDataSet 数据
     */
    BinlogDataSet getBinlogDataSet();
}
