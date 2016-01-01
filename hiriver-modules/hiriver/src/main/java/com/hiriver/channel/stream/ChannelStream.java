package com.hiriver.channel.stream;

/**
 * 描述从一个mysql数据源接收数据并被消费者消费的一个整体的流程。
 * 
 * @author hexiufeng
 *
 */
public interface ChannelStream {
    /**
     * 开启数据接收
     */
     void start();
     /**
      * 是否资源
      */
     void release();
}
