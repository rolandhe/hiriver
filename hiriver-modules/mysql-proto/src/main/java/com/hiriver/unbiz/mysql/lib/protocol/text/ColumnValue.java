package com.hiriver.unbiz.mysql.lib.protocol.text;

import com.hiriver.unbiz.mysql.lib.MyCharset;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;

/**
 * 描述列的值
 * 
 * @author hexiufeng
 *
 */
public class ColumnValue {
    private final ColumnValueProvider provider;
    private final ColumnDefinition definition;

    public ColumnValue(ColumnValueProvider provider, ColumnDefinition definition) {
        this.provider = provider;
        this.definition = definition;
        provider.useCharset(definition.getCharset());
    }

    public boolean isNull() {
        return provider.isNull();
    }

    public String getValueAsString() {
        return provider.getValueAsString();
    }

    public Integer getValueAsInt() {
        return provider.getValueAsInt();
    }

    public Long getValueAsLong() {
        return provider.getValueAsLong();
    }

    public MyCharset getCharset() {
        return definition.getCharset();
    }
    
    public String getColumnName(){
        return definition.getColumName();
    }
}
