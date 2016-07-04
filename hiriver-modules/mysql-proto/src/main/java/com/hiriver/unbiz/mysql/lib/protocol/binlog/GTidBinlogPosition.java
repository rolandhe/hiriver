package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiriver.unbiz.mysql.lib.protocol.Request;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * 基于gtid的同步点实现，适用于mysql5.6.9之后的版本
 * 
 * @author hexiufeng
 *
 */
public class GTidBinlogPosition implements BinlogPosition {
    private static final Logger LOGGER = LoggerFactory.getLogger(GTidBinlogPosition.class);
    private GtIdSet gtidset;

    public GTidBinlogPosition() {
    }

    public GTidBinlogPosition(String gtIdSetString) {
        this.gtidset = new GtIdSet(gtIdSetString);
    }

    @Override
    public Request packetDumpRequest(int serverId) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("dump binlog use gtid set {}.", gtidset);
        }
        GTidDumpRequest request = new GTidDumpRequest(gtidset.getGtidMap(), serverId);
        return request;
    }

    @Override
    public byte[] toBytesArray() {
        return toString().getBytes();
    }

    public GtIdSet getGtidset() {
        return gtidset;
    }

    public void setGtidset(GtIdSet gtidset) {
        this.gtidset = gtidset;
    }

    public GTidBinlogPosition fixConfPos() {
        Map<String, GtId> gtIdMap = gtidset.cloneGtIdMap();
        StringBuilder sb = new StringBuilder();
        int count = 0;
        for (String uuid : gtIdMap.keySet()) {
            if (count < gtIdMap.size() - 1) {
                sb.append(gtIdMap.get(uuid).cloneNextGtId().toString());
            } else {
                sb.append(gtIdMap.get(uuid).cloneGtId().toString());
            }
            sb.append(",");
            count++;
        }
        return new GTidBinlogPosition(sb.substring(0, sb.length() - 1));
    }

    @Override
    public String toString() {
        return gtidset.toString();
    }

    @Override
    public boolean isSame(BinlogPosition posStore) {
        if (!(posStore instanceof GTidBinlogPosition)) {
            return false;
        }
        return toString().equals(posStore.toString());
    }

}
