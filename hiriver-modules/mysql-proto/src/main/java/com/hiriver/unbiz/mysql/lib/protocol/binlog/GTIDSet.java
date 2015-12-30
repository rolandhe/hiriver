package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 描述一组gtid，gtid之间以逗号分隔
 * 
 * @author hexiufeng
 *
 */
public class GTIDSet {
    private final Map<String, GTIDInfo> map = new LinkedHashMap<String, GTIDInfo>();

    /**
     * 构造器
     * 
     * @param gtidSetString uuid:12,uuid:13
     */
    public GTIDSet(String gtidSetString) {
        String[] array = gtidSetString.split(",");
        for (String gtidInfo : array) {
            GTIDInfo gi = new GTIDInfo(gtidInfo);
            map.put(gi.getUuid(), gi);
        }
    }

    public void updateGTIDPoint(String uuid, long point) {
        if (map.containsKey(uuid)) {
            map.get(uuid).setStop(point);
        }
    }

    public GTIDInfo[] getAllGTIDSet() {
        return map.values().toArray(new GTIDInfo[0]);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (GTIDInfo gi : map.values()) {
            sb.append(gi.toString());
            sb.append(",");
        }
        return sb.substring(0, sb.length() - 1);
    }
}
