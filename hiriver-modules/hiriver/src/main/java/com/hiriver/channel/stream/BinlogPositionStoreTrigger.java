package com.hiriver.channel.stream;

/**
 * 用于触发记录同步点的触发器，用于使用了{@link ChannelBuffer}提高性能，需要消费者触发何时来
 * 记录同步点，但记录同步点的具体操作不应该暴露给消费者，因此使用回调模式处理，BinlogPositionStoreTrigger
 * 就是回调的描述
 * 
 * @author hexiufeng
 *
 */
public interface BinlogPositionStoreTrigger {
    /**
     * 记录同步点操作
     */
    void triggerStoreBinlogPos();
}
