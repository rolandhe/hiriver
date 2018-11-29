package com.hiriver.channel.stream.impl;

import com.hiriver.channel.stream.BufferableBinlogDataSet;
import com.hiriver.channel.stream.ChannelBuffer;
import com.hiriver.unbiz.mysql.lib.output.BinlogResultRow;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 * 精确控制BinlogResultRow个数，BinlogDataSet包含太多的BinlogResultRow导致的内存不受控
 * </pre>
 * <p>
 * created by Yang Huawei (xander.yhw@alibaba-inc.com) on 2018/9/5 01:33
 */
public class LimitByRowsChannelBuffer implements ChannelBuffer {

    private volatile boolean init = false;
    private BlockingQueue<BufferableBinlogDataSetWithRowCount> queue;
    private Semaphore semaphore;

    private int limit = 128;

    public LimitByRowsChannelBuffer(int limit) {
        this.limit = limit;
    }

    public LimitByRowsChannelBuffer() {
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    private void initIfNeed() {
        if (init) {
            return;
        }
        synchronized (this) {
            if (init) {
                return;
            }
            semaphore = new Semaphore(limit);
            queue = new LinkedBlockingQueue<>(limit);
            init = true;
        }
    }

    @Override
    public boolean push(BufferableBinlogDataSet ds, long timeout, TimeUnit timeUnit) {
        initIfNeed();
        int sizeSum = 0;
        if (ds.getBinlogDataSet() != null && ds.getBinlogDataSet().getRowDataMap() != null) {
            for (List<BinlogResultRow> binlogResultRows : ds.getBinlogDataSet().getRowDataMap()
                    .values()) {
                sizeSum += binlogResultRows.size();
            }
        }

        sizeSum = Math.min(sizeSum, limit);// 防止ds包含的row数量超过limit而在此死锁

        try {
            if (sizeSum > 0) {
                if (!semaphore.tryAcquire(sizeSum, timeout, timeUnit)) {
                    return false;
                }
            }
            return queue.offer(new BufferableBinlogDataSetWithRowCount(ds, sizeSum), timeout, timeUnit);// 不在重新计算timeout，一般来说瓶颈在semaphore；
        } catch (InterruptedException e) {
            // ignore
            return false;
        }
    }

    @Override
    public BufferableBinlogDataSet pop(long timeout, TimeUnit timeUnit) {
        initIfNeed();
        try {
            BufferableBinlogDataSetWithRowCount bufferableBinlogDataSetWithRowCount =
                    queue.poll(timeout, timeUnit);
            if (bufferableBinlogDataSetWithRowCount == null) {
                return null;
            }
            if (bufferableBinlogDataSetWithRowCount.size > 0) {
                semaphore.release(bufferableBinlogDataSetWithRowCount.size);
            }
            return bufferableBinlogDataSetWithRowCount.bufferableBinlogDataSet;
        } catch (InterruptedException e) {
            // ignore
            return null;
        }
    }

    private class BufferableBinlogDataSetWithRowCount {
        private final BufferableBinlogDataSet bufferableBinlogDataSet;
        private final int size;

        BufferableBinlogDataSetWithRowCount(BufferableBinlogDataSet bufferableBinlogDataSet, int size) {
            this.bufferableBinlogDataSet = bufferableBinlogDataSet;
            this.size = size;
        }
    }

    public int availablePermits() {
        initIfNeed();
        return this.semaphore.availablePermits();
    }
}
