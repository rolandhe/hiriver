package com.hiriver.position.store.impl;

import com.hiriver.position.store.BinlogPositionStore;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogFileBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.GTidBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TimestampBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 抽象的存储的同步点实现
 * 
 * @author hexiufeng
 *
 */
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

        if(line.isEmpty()){
          return null;
        }
        
        try{
            return new GTidBinlogPosition(line);
        }catch(RuntimeException e){
            LOG.info("loaded binlog pos [{}] from store may be binlog name+pos in {},",line, channelId);
        }
        
        String[] array = line.split(":");
        if (array.length == 2) {
          return new BinlogFileBinlogPosition(array[0], Long.parseLong(array[1]));
         
        }else if (array.length==4){
          return new TimestampBinlogPosition(Long.parseLong(array[0]), emptyToNull(array[1]),
              emptyToNull(array[2]),
              StringUtils.equals(array[3], "") ? null : Long.parseLong(array[3]));
        }else if (array.length==1){
          return new TimestampBinlogPosition(Long.parseLong(array[0]));
        }else{
          LOG.info("loaded binlog pos [{}] from store is incorrect in {},",line, channelId);
          return null;
        }

    }

    private String emptyToNull(String string) {
        if (StringUtils.equals(string, "")) {
            return null;
        }
        return string;
    }

    /**
     * 存储同步点
     * 
     * @param posBuf 二进制化的同步点
     * @param channelId 指定的数据流
     */
    protected abstract void storeImpl(byte[] posBuf, String channelId);

    /**
     * 加载二进制化的同步点
     * 
     * @param channelId 指定的数据流
     * @return 同步点
     */
    protected abstract byte[] loadImpl(String channelId);
}
