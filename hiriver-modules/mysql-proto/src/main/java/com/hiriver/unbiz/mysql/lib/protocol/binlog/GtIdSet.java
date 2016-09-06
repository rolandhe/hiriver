package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * gtid set 描述
 * 
 * @author hexiufeng
 *
 */
public class GtIdSet {
    private final Map<String, GtId> gtidMap = new LinkedHashMap<>();

    /**
     * uuid:[2-]13,uuid:[3-]19
     * 
     * @param gtIdSetString gtid st string
     */
    public GtIdSet(String gtIdSetString) {
        gtIdSetString = gtIdSetString.replace("\n", "").replace(" ", "");
        String[] gtidArray = gtIdSetString.split(",");
        for (String gtid : gtidArray) {
            GtId gi =  new GtId(gtid);
            if (gtidMap.containsKey(gi.getUuid())) {
                throw new RuntimeException("uuid must be unique.");
            }
            gtidMap.put(gi.getUuid(), gi);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String uuid : this.gtidMap.keySet()) {
            GtId gi = gtidMap.get(uuid);
            sb.append(gi.toString());
            sb.append(",");
        }
        return sb.substring(0, sb.length() - 1);
    }

    public Map<String, GtId> getGtidMap() {
        return Collections.unmodifiableMap(gtidMap);
    }

    public Map<String, GtId> cloneGtIdMap() {
        Map<String, GtId> clone = new LinkedHashMap<>();
        for (String uuid : gtidMap.keySet()) {
            clone.put(uuid, gtidMap.get(uuid).cloneGtId());
        }
        return clone;
    }
}
