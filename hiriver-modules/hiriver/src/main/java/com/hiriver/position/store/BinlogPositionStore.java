package com.hiriver.position.store;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * 同步点存储抽象描述
 * 
 * @author hexiufeng
 *
 */
public interface BinlogPositionStore {
    /**
     * 存储同步点
     * @param binlogPosition 同步点
     * @param channelId 同步点所属的数据流
     */
    void store(BinlogPosition binlogPosition, String channelId);

    /**
     * 加载指定数据流的同步点
     * 
     * @param channelId 指定的数据流
     * @return 同步点
     */
    BinlogPosition load(String channelId);
}
