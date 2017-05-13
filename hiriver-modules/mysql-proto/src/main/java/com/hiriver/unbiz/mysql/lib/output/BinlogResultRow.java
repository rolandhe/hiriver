package com.hiriver.unbiz.mysql.lib.output;

import java.util.List;

/**
 * 行数据的描述
 * 
 * @author hexiufeng
 *
 */
public class BinlogResultRow {
    /**
     * delete、update事件有效
     */
    private final List<BinlogColumnValue> beforeColumnValueList;
    /**
     * insert、update事件有效
     */
    private final List<BinlogColumnValue> afterColumnValueList;
    private final RowModifyTypeEnum rowModifyType;
    /**
     * 当前数据写入数据库时的时间戳，unix 时间戳
     */
    private final long binlogOccurTime;

    public BinlogResultRow(List<BinlogColumnValue> beforeColumnValueList, List<BinlogColumnValue> afterColumnValueList,
            RowModifyTypeEnum rowModifyType, long binlogOccurTime) {
        this.beforeColumnValueList = beforeColumnValueList;
        this.afterColumnValueList = afterColumnValueList;
        this.rowModifyType = rowModifyType;
        this.binlogOccurTime = binlogOccurTime;
    }

    public List<BinlogColumnValue> getBeforeColumnValueList() {
        return beforeColumnValueList;
    }

    public List<BinlogColumnValue> getAfterColumnValueList() {
        return afterColumnValueList;
    }

    public RowModifyTypeEnum getRowModifyType() {
        return rowModifyType;
    }

    public long getBinlogOccurTime() {
        return binlogOccurTime;
    }

}
