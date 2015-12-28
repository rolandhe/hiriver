package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.ColumnType;

public class InternelColumnDefinition {
    private ColumnType columnType;
    private int meta;
    private boolean isNull;

    public InternelColumnDefinition(ColumnType columnType, int meta, boolean isNull) {
        this.columnType = columnType;
        this.meta = meta;
        this.isNull = isNull;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public int getMeta() {
        return meta;
    }

    public boolean isNull() {
        return isNull;
    }
}
