package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.ColumnType;
import com.hiriver.unbiz.mysql.lib.protocol.tool.GenericStringTypeChecker;

/**
 * 内部使用的表的列定义
 * 
 * @author hexiufeng
 *
 */
public class InternelColumnDefinition {
    private ColumnType columnType;
    /**
     * 主从复制协议中定义的每个字段的元数据，TableMapEvent会定义每一列的元数据，
     * 而元数据所占的长度跟类型有关，参见http://dev.mysql.com/doc/internals/en/table-map-event.html
     */
    private int meta;
    private boolean isNull;

    /**
     * 该字段是否是枚举或者set
     */
    private boolean enumOrSet;

    public InternelColumnDefinition(ColumnType columnType, int meta, boolean isNull) {
        this.columnType = columnType;
        this.meta = meta;
        this.isNull = isNull;
        // 枚举或set在内部也用string表示，需要区分开来
        if(columnType == ColumnType.MYSQL_TYPE_STRING){
            int[] lenghHolder = {0};
            ColumnType realType = GenericStringTypeChecker.checkRealColumnType(meta,lenghHolder);
            if(realType == ColumnType.MYSQL_TYPE_ENUM || realType == ColumnType.MYSQL_TYPE_SET){
                enumOrSet = true;
            }
        }
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

    public boolean isEnumOrSet(){
        return enumOrSet;
    }
}
