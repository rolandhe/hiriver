package com.hiriver.channel;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.hiriver.unbiz.mysql.lib.output.BinlogResultRow;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;

/**
 * 从binlog中解析出来的数据描述。可能是下列情况之一：<br>
 * <ul>
 * <li> 从binlog的row event解析出得数据。{@link BinlogDataSet#getIsPositionStoreTrigger()}必须是<b>false</b></li>
 * <li> 用于通知消费者{@link com.hiriver.channel.stream.Consumer}当前事务已经结束，可以记录同步点的信号数据。
 * {@link BinlogDataSet#getIsPositionStoreTrigger()}必须是<b>true</b></li>
 * </ul>
 * 
 * @author hexiufeng
 *
 */
public final class BinlogDataSet {
    /**
     * 描述数据来自哪个channel，一个channel描述一个mysql数据源.
     * 当多个数据源数据发送到同一个{@link com.hiriver.channel.stream.ChannelBuffer}以便消费者
     * 消费时特别有用，比如可以用于排查异常问题
     */
    private final String channelId;
    /**
     * 数据来自于哪个mysql实例。format is: host:port
     */
    private final String sourceHostUrl;
    /**
     * 当数据所在事务的gtid。在不支持gtid模式下，为null
     */
    private final String gtId;

    /**
     * 当前数据所在事务的binlogfile + pos
     */
    private final String binlogPos;
    /**
     * 是否是通知消费者记录同步点的信号数据包。如果是信号数据包：<br>
     * <ul>
     * <li>{@link BinlogDataSet#columnDefMap} 不包含任何数据 ，即columnDefMap.size() = 0</li>
     * <li>{@link BinlogDataSet#rowDataMap} 不包含任何数据,即rowDataMap.size() = 0 </li>
     * </ul>
     * 
     */
    private final boolean isPositionStoreTrigger;
    
    /**
     * 事务是否结束
     */
    private final boolean isStartTransEvent;

    /**
     * 描述表对应的列信息，key 是表名称，dbname.tablename格式
     */
    private final Map<String, List<ColumnDefinition>> columnDefMap = new HashMap<>();
    /**
     * 描述表对应的行数据，key 是表名称，dbname.tablename格式
     */
    private final Map<String, List<BinlogResultRow>> rowDataMap = new LinkedHashMap<>();

    public static BinlogDataSet createPositionStoreTrigger(String channelId, String sourceHostUrl, String gtId,String
            binlogPos) {
        return new BinlogDataSet(channelId, sourceHostUrl, gtId,binlogPos, true,false);
    }
    
    public static BinlogDataSet createStartTransEvent(String channelId, String sourceHostUrl, String gtId,String
            binlogPos) {
        return new BinlogDataSet(channelId, sourceHostUrl, gtId, binlogPos, false,true);
    }

    public BinlogDataSet(String channelId, String sourceHostUrl, String gtId,String
            binlogPos) {
        this(channelId, sourceHostUrl, gtId, binlogPos,false,false);
    }

    private BinlogDataSet(String channelId, String sourceHostUrl, String gtId,String binlogPos, boolean
            isPositionStoreTrigger,
                          boolean isStartTransEvent) {
        this.channelId = channelId;
        this.sourceHostUrl = sourceHostUrl;
        this.gtId = gtId;
        this.binlogPos = binlogPos;
        this.isPositionStoreTrigger = isPositionStoreTrigger;
        this.isStartTransEvent = isStartTransEvent;
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

    public String getBinlogPos(){
        return binlogPos;
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

    public boolean isStartTransEvent() {
        return isStartTransEvent;
    }
}
