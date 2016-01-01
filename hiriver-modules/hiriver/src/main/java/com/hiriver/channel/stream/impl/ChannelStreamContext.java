package com.hiriver.channel.stream.impl;

import java.util.concurrent.CountDownLatch;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * 在一个{@link  com.hiriver.channel.stream.ChannelStream}过程中一些通用信息
 * 的存储
 * 
 * @author hexiufeng
 *
 */
final class ChannelStreamContext {
    /**
     * 当与mysql链接断开后重连时的同步点位置，之所以不能直接使用{@link com.hiriver.position.store.BinlogPositionStore}
     * 的位置是因为{@link com.hiriver.channel.stream.ChannelBuffer}的存在，如果使用了，ChannelBuffer内的数据会被重新发送
     */
    private BinlogPosition nextPos;
    /**
     * 从mysql接收binlog线程关闭完成通知。线程会判断shutDownTrigger == true
     */
    final CountDownLatch shutDownProviderEvent = new CountDownLatch(1);
    /**
     * 消费线程关闭完成通知。线程会判断shutDownTrigger == true
     */
    final CountDownLatch shutDownConsumerEvent = new CountDownLatch(1);
    /**
     * {@link com.hiriver.channel.stream.ChannelStream}关闭时通知binlog接收线程和消费线程
     * 关闭的信号
     */
    volatile boolean shutDownTrigger = false;

    public BinlogPosition getNextPos() {
        return nextPos;
    }

    public void setNextPos(BinlogPosition currentPos) {
        this.nextPos = currentPos;
    }
}
