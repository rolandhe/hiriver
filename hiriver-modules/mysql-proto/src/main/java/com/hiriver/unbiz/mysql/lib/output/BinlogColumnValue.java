package com.hiriver.unbiz.mysql.lib.output;

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
