package com.hiriver.channel.stream;

import com.hiriver.channel.BinlogDataSet;

/**
 * 业务方消费binlog数据的抽象描述.
 * <b>强烈推荐<b>业务方式继承{@link com.hiriver.channel.stream.impl.AbstractConsumer}
 * 
 * 
 * @author hexiufeng
 *
 */
public interface Consumer {

    /**
     * 
     * 描述消费binlog数据的接口。
     * 
     * <b>注意:<b><br>
     * <ul>
     * <li>要在本方法的实现中处理所有异常。如果没有处理，内部消费者线程会被中断。</li>
     * <li>消费数据失败，需要在本方法中自行处理，比如记录日之后，跳过本条数据</li>
     * </ul>
     * 
     * @param ds 一般是从binlog中解析的数据，也可能是空数据，用于标示需要记录同步点标示数据， 通过{@link BinlogDataSet#getIsPositionStoreTrigger()}=true来识别
     * @param storeTrigger 记录同步点的触发器，当{@link BinlogDataSet#getIsPositionStoreTrigger()}=true被调用
     */
    void consumer(final BinlogDataSet ds, final BinlogPositionStoreTrigger storeTrigger);
}
