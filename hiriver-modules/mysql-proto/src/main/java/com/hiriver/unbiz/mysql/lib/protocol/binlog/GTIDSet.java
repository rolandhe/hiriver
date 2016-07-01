package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 描述一组gtid，gtid之间以逗号分隔
 * 
 * @author hexiufeng
 *
 */
public class GTIDSet {
    private final Map<String, List<GTIDInfo>> map = new LinkedHashMap<>();

    /**
     * 构造器
     * 
     * @param gtidSetString uuid:[3-]12,uuid:[3-]13
     */
    public GTIDSet(String gtidSetString) {
        gtidSetString = gtidSetString.replaceFirst("\n", "");
        String[] array = gtidSetString.split(",");
        for (String gtidInfo : array) {
            GTIDInfo gi = new GTIDInfo(gtidInfo);
            List<GTIDInfo> internalList = null;
            if (map.containsKey(gi.getUuid())) {
                internalList = map.get(gi.getUuid());
            } else {
                internalList = new ArrayList<>();
                map.put(gi.getUuid(), internalList);
            }
            internalList.add(gi);
        }
    }

    public Map<String, List<GTIDInfo>> getAllGTIDSet() {
       return  Collections.unmodifiableMap(map);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (List<GTIDInfo> giList : map.values()) {
            for (GTIDInfo gi : giList) {
                sb.append(gi.toShortString());
                sb.append(",");
            }
        }
        return sb.substring(0, sb.length() - 1);
    }
}
