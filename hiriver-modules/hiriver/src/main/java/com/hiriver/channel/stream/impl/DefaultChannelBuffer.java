package com.hiriver.channel.stream.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import com.hiriver.channel.stream.BufferableBinlogDataSet;
import com.hiriver.channel.stream.ChannelBuffer;

/**
 * 缺省的 {@link ChannelBuffer}实现，内部使用{@link LinkedBlockingQueue}实现，支持设定上限。
 * ChannelBuffer必须设定上限，否则会打爆内存，缺省上限是5000.
 * 
 * @author hexiufeng
 *
 */
public class DefaultChannelBuffer implements ChannelBuffer {
    private int limit = 5000;
    private BlockingQueue<BufferableBinlogDataSet> queue = new LinkedBlockingQueue<>(limit) ;
    
    public int getLimit() {
        return limit;
    }
    public void setLimit(int limit) {
        this.limit = limit;
    }
    @PostConstruct
    public void init(){
        queue = new LinkedBlockingQueue<>(limit);
    }
    @Override
    public boolean push(BufferableBinlogDataSet ds,long timeout, TimeUnit timeUnit) {
        try {
            return queue.offer(ds, timeout, timeUnit);
        } catch (InterruptedException e) {
            // ignore
            return false;
        }
    }

    @Override
    public BufferableBinlogDataSet pop(long timeout, TimeUnit timeUnit) {
        try {
            return queue.poll(timeout, timeUnit);
        } catch (InterruptedException e) {
            // ignore
            return null;
        }
    }

}
