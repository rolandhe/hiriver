package com.hiriver.channel.stream;

import java.util.concurrent.TimeUnit;

/**
 * 为提高性能用于缓冲binlog事件数据的缓冲区，用于解耦binlog接收线程和数据消费线程,线程安全
 * 
 * @author hexiufeng
 *
 */
public interface ChannelBuffer {
    /**
     * 发送数据，支持超时
     * 
     * @param ds BufferableBinlogDataSet数据
     * @param timeout 预期超时
     * @param timeUnit 超时单位
     * @return 是否发送成功
     */
    boolean push(BufferableBinlogDataSet ds,long timeout,TimeUnit timeUnit);
    /**
     * 从缓冲区中读取数据，先进先出。支持超时
     * 
     * @param timeout 预期超时
     * @param timeUnit 超时单位
     * @return 读取到的数据，当缓冲区为空时，返回null
     */
    BufferableBinlogDataSet pop(long timeout,TimeUnit timeUnit);
}
