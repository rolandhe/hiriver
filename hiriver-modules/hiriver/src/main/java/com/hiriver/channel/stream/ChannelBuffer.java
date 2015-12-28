package com.hiriver.channel.stream;

import java.util.concurrent.TimeUnit;

public interface ChannelBuffer {
    boolean push(BufferableBinlogDataSet ds,long timeout,TimeUnit timeUnit);
    BufferableBinlogDataSet pop(long timeout,TimeUnit timeUnit);
}
