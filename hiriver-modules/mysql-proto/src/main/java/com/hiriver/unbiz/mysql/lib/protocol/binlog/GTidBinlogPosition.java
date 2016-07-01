package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
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
    private GTIDSet gtidset;

    public GTidBinlogPosition() {
    }

    public GTidBinlogPosition(String gtIdSetString) {
        this.gtidset = new GTIDSet(gtIdSetString);
    }
    // public GTidBinlogPosition(GTIDSet gtidset) {
    // this.gtidset = gtidset;
    // }

    @Override
    public Request packetDumpRequest(int serverId, String extendGtId) {
        Map<String, List<GTIDInfo>> gtIdMap = mergeGtIdSetInfo(extendGtId);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("dump binlog use gtid set {}.",convertGtIdMap2String(gtIdMap));
        }
        GTidDumpRequest request = new GTidDumpRequest(gtIdMap, serverId);
        return request;
    }

    private String convertGtIdMap2String(Map<String, List<GTIDInfo>> gtIdMap) {
        StringBuilder sb = new StringBuilder();
        for (String uuid : gtIdMap.keySet()) {
            List<GTIDInfo> list = gtIdMap.get(uuid);
            for (GTIDInfo gi : list) {
                sb.append(gi.toString());
                sb.append(",");
            }
        }
        return sb.substring(0, sb.length() - 1);
    }

    private Map<String, List<GTIDInfo>> mergeGtIdSetInfo(String extendGtId) {
        if (extendGtId == null || extendGtId.length() == 0) {
            return gtidset.getAllGTIDSet();
        }
        LOGGER.info("read Executed_Gtid_Set from db: {}.",extendGtId);
        GTIDSet extend = new GTIDSet(extendGtId);
        Map<String, List<GTIDInfo>> extendMap = extend.getAllGTIDSet();
        Map<String, List<GTIDInfo>> currentMap = gtidset.getAllGTIDSet();
        String currentUuid = currentMap.keySet().iterator().next();
        GTIDInfo currentGi = currentMap.get(currentUuid).get(0);

        Map<String, List<GTIDInfo>> mergedMap = new LinkedHashMap<>();
        
//        List<String> uuidList = new ArrayList<>(extendMap.keySet().size());
//        uuidList.addAll(extendMap.keySet());
//        Collections.reverse(uuidList);
        
        for (String uuid : extendMap.keySet()) {
            if (!uuid.equalsIgnoreCase(currentUuid)) {
                List<GTIDInfo> oldGiList = extendMap.get(uuid);
                List<GTIDInfo> newGiList = new ArrayList<>(oldGiList.size());
                for(GTIDInfo gi : oldGiList){
                    newGiList.add(new GTIDInfo(gi.getUuid(),gi.getStart(),gi.getStop() + 1));
                }
                mergedMap.put(uuid, newGiList);
                continue;
            }
            List<GTIDInfo> list = new ArrayList<>();
            List<GTIDInfo> extList = extendMap.get(uuid);
            for (GTIDInfo gi : extList) {
                if (gi.getStop() < currentGi.getStop()) {
                    list.add(new GTIDInfo(gi.getUuid(),gi.getStart(),gi.getStop() + 1));
                    continue;
                }
                long stop = currentGi.getStop();
                list.add(new GTIDInfo(uuid, gi.getStart(), stop));
                break;
            }
            mergedMap.put(uuid, list);
        }

        return mergedMap;
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
