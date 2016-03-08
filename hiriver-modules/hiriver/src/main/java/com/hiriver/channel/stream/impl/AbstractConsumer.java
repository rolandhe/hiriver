package com.hiriver.channel.stream.impl;

import com.hiriver.channel.BinlogDataSet;
import com.hiriver.channel.stream.BinlogPositionStoreTrigger;
import com.hiriver.channel.stream.Consumer;

/**
 * {@link com.hiriver.channel.stream.impl.Consumer}抽象实现，强烈建议业务方继承本类实现自己的业务处理。
 * 
 * @author hexiufeng
 *
 */
public abstract class AbstractConsumer implements Consumer {

    @Override
    public final void consume(final BinlogDataSet ds, final BinlogPositionStoreTrigger storeTrigger) {
        if(ds.getIsPositionStoreTrigger()){
            storeTrigger.triggerStoreBinlogPos();
            return;
        }
        if(ds.isStartTransEvent()){
            return;
        }
        consumeRowData(ds);
    }
    /**
     * 消费使用binlog数据的抽象方法，由业务方实现.<br>
     * 
     * <b>注意:<b><br>
     * <ul>
     * <li>要在本方法的实现中处理所有异常。如果没有处理，内部消费者线程会被中断。</li>
     * <li>消费数据失败，需要在本方法中自行处理，比如记录日之后，跳过本条数据</li>
     * </ul>
     * 
     * @param rowData 从binlog识别出来的数据
     */
    protected abstract void  consumeRowData(final BinlogDataSet rowData);
}
