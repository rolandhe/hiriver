package com.hiriver.channel.stream.impl;

import java.util.Map;

import com.hiriver.channel.stream.TransactionRecognizer;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.GTidBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.GtId;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidEventType;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.GTidEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * gtid方式的事务识别器。gtid方式下在事务开启后的第一事件就是GTID事件，同时GTID的不同标示者着上一个事务的结束。 但我们并不使用该特性来判断事务是否结束
 * 
 * @author hexiufeng
 *
 */
public class GTIDTransactionRecognizer extends AbstractTransactionRecognizer implements TransactionRecognizer {
    private String gtIdString;
    private Map<String, GtId> gtIdMap;

    @Override
    public boolean tryRecognizePos(ValidBinlogOutput validOutput) {
        String newGtId = getNewGtId(validOutput);
        if (newGtId != null) {
            gtIdString = newGtId;
            applyGtIdString(newGtId);
            return true;
        }
        return false;
    }

    @Override
    public BinlogPosition getCurrentTransBeginPos() {
        if (gtIdString == null) {
            return null;
        }
        return getFullGtIdString();
    }

    @Override
    public String getGTId() {
        return gtIdString;
    }

    private GTidBinlogPosition getFullGtIdString() {
        StringBuilder sb = new StringBuilder();
        for (String uuid : gtIdMap.keySet()) {
            sb.append(gtIdMap.get(uuid).cloneGtId().toString());
            sb.append(",");
        }
        return new GTidBinlogPosition(sb.substring(0, sb.length() - 1));
    }

    private void applyGtIdString(String gtid) {
        GtId gi = new GtId(gtid);
        if(gtIdMap.containsKey(gi.getUuid())){
            gtIdMap.remove(gi.getUuid());
        }
        gtIdMap.put(gi.getUuid(), gi);
    }

    public final void useInitPos(GTidBinlogPosition pos) {
        gtIdMap = pos.getGtidset().cloneGtIdMap();
    }

    private String getNewGtId(ValidBinlogOutput validOutput) {
        if (validOutput.getEventType() == ValidEventType.GTID) {
            GTidEvent gtEvent = (GTidEvent) (validOutput.getEvent());
            return gtEvent.getGTidString();
        }
        return null;
    }

}
