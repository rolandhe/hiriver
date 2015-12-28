package com.hiriver.unbiz.mysql.lib.protocol.text;

import com.hiriver.unbiz.mysql.lib.MyCharset;

public interface ColumnValueProvider {
    String getValueAsString();

    Integer getValueAsInt();

    Long getValueAsLong();

    boolean isNull();

    void useCharset(MyCharset charset);
}
