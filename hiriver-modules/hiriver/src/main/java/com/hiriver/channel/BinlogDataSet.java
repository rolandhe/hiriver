package com.hiriver.channel;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.hiriver.unbiz.mysql.lib.output.BinlogResultRow;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;

public final class BinlogDataSet {
    private final String channelId;
    private final String sourceHostUrl;
    private final String gtId;
    private final boolean isPositionStoreTrigger;

    private final Map<String, List<ColumnDefinition>> columnDefMap = new HashMap<>();
    private final Map<String, List<BinlogResultRow>> rowDataMap = new LinkedHashMap<>();

    public static BinlogDataSet createPositionStoreTrigger(String channelId, String sourceHostUrl, String gtId) {
        return new BinlogDataSet(channelId, sourceHostUrl, gtId, true);
    }

    public BinlogDataSet(String channelId, String sourceHostUrl, String gtId) {
        this(channelId, sourceHostUrl, gtId, false);
    }

    private BinlogDataSet(String channelId, String sourceHostUrl, String gtId, boolean isPositionStoreTrigger) {
        this.channelId = channelId;
        this.sourceHostUrl = sourceHostUrl;
        this.gtId = gtId;
        this.isPositionStoreTrigger = isPositionStoreTrigger;
    }

    public String getChannelId() {
        return channelId;
    }

    public String getSourceHostUrl() {
        return sourceHostUrl;
    }

    public String getGtId() {
        return gtId;
    }

    public Map<String, List<ColumnDefinition>> getColumnDefMap() {
        return columnDefMap;
    }

    public Map<String, List<BinlogResultRow>> getRowDataMap() {
        return rowDataMap;
    }

    public boolean getIsPositionStoreTrigger() {
        return isPositionStoreTrigger;
    }
}
