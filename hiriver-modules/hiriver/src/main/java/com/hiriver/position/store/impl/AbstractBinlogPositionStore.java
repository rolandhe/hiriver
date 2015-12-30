package com.hiriver.position.store.impl;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiriver.position.store.BinlogPositionStore;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogFileBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.GTidBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

public abstract class AbstractBinlogPositionStore implements BinlogPositionStore {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractBinlogPositionStore.class);

    @Override
    public void store(BinlogPosition binlogPosition, String channelId) {
        storeImpl(binlogPosition.toBytesArray(), channelId);
    }

    @Override
    public BinlogPosition load(String channelId) {
        byte[] posBuf = loadImpl(channelId);
        if (posBuf == null) {
            LOG.info("can't load binlog pos from store in {}", channelId);
            return null;
        }
        String line = new String(posBuf);
        line = line.trim();
        String[] array = line.split(":");
        if (array.length != 2) {
            LOG.info("loaded binlog pos [{}] from store is incorrect in {},",line, channelId);
            return null;
        }
        try{
            UUID.fromString(array[0]);
            return new GTidBinlogPosition(line);
        }catch(IllegalArgumentException e){
            LOG.info("loaded binlog pos [{}] from store may be binlog name+pos in {},",line, channelId);
        }
        
        return new BinlogFileBinlogPosition(array[0], Long.parseLong(array[1]));
    }

    protected abstract void storeImpl(byte[] posBuf, String channelId);

    protected abstract byte[] loadImpl(String channelId);
}
