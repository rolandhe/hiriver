package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import java.util.ArrayList;
import java.util.List;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;

public class TableMeta {
    private final List<ColumnDefinition> columnMetaList = new ArrayList<ColumnDefinition>();
    private final long tableId;

    public TableMeta(long tableId) {
        this.tableId = tableId;
    }

    public void addColumn(ColumnDefinition meta) {
        this.columnMetaList.add(meta);
    }

    public List<ColumnDefinition> getColumnMetaList() {
        return columnMetaList;
    }

    public ColumnDefinition getColumnDefinition(int index) {
        return this.columnMetaList.get(index);
    }

    public long getTableId() {
        return tableId;
    }

}
