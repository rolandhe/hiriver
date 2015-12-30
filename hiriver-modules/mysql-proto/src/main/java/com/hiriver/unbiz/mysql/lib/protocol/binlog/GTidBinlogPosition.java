package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.Request;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * 基于gtid的同步点实现，适用于mysql5.6.9之后的版本
 * 
 * @author hexiufeng
 *
 */
public class GTidBinlogPosition implements BinlogPosition {
    private GTIDSet gtidset;

    public GTidBinlogPosition() {
    }
    public GTidBinlogPosition(String gtIdSetString) {
        this.gtidset = new GTIDSet(gtIdSetString);
    }
    public GTidBinlogPosition(GTIDSet gtidset) {
        this.gtidset = gtidset;
    }

    @Override
    public Request packetDumpRequest(int serverId) {
        GTidDumpRequest request = new GTidDumpRequest(gtidset, serverId);
        return request;
    }

    @Override
    public byte[] toBytesArray() {
        return toString().getBytes();
    }


    public GTIDSet getGtidset() {
        return gtidset;
    }

    public void setGtidset(GTIDSet gtidset) {
        this.gtidset = gtidset;
    }
    

    @Override
    public String toString(){
        return gtidset.toString();
    }
    @Override
    public boolean isSame(BinlogPosition posStore) {
        if(!(posStore instanceof GTidBinlogPosition)){
            return false;
        }
        return toString().equals(posStore.toString());
    }

}
