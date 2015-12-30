package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import java.util.ArrayList;
import java.util.List;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;

/**
 * table的元数据描述，主要包含字段的定义，包括字段名称，类型、是否为主键等
 * 
 * @author hexiufeng
 *
 */
public class TableMeta {
    private final List<ColumnDefinition> columnMetaList = new ArrayList<ColumnDefinition>();
    /**
     * tableid,mysql主从复制中的概念，标示一个表的唯一id，确切的说是mysql server缓存中针对每一个表分配的id，同一个表的id会变化.<br>
     * <ul>
     * <li>当mysql缓存刷新时，一个的表的id会发生变化</li>
     * <li>当表结构修改是，tableid会发生变化</li
     * </ul>
     */
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
