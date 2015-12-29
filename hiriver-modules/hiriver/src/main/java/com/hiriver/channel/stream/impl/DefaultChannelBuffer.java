package com.hiriver.channel.stream.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import com.hiriver.channel.stream.BufferableBinlogDataSet;
import com.hiriver.channel.stream.ChannelBuffer;

public class DefaultChannelBuffer implements ChannelBuffer {
    private int limit = 1;
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
