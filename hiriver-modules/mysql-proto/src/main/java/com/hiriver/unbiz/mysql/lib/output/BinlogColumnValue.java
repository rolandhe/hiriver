package com.hiriver.unbiz.mysql.lib.output;

/**
 * 对上层暴露数据时，对每个字段的值的描述
 * 
 * @author hexiufeng
 *
 */
public class BinlogColumnValue {
    private final ColumnDefinition definition;
    private final Object value;

    public BinlogColumnValue(ColumnDefinition definition, Object value) {
        this.definition = definition;
        this.value = value;
    }

    public ColumnDefinition getDefinition() {
        return definition;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        String strValue =  value == null ? "null" : value.toString();
        return definition.getColumName() + ":" + strValue;
    }
}
