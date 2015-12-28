package com.hiriver.channel.stream.impl;

import java.util.concurrent.CountDownLatch;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

final class ChannelStreamContext {
    private BinlogPosition nextPos;
    final CountDownLatch shutDownProviderEvent = new CountDownLatch(1);
    final CountDownLatch shutDownConsumerEvent = new CountDownLatch(1);
    volatile boolean shutDownTrigger = false;

    public BinlogPosition getNextPos() {
        return nextPos;
    }

    public void setNextPos(BinlogPosition currentPos) {
        this.nextPos = currentPos;
    }
}
