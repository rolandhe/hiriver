package com.hiriver.unbiz.mysql.lib.protocol.text;

import com.hiriver.unbiz.mysql.lib.MyCharset;
import com.hiriver.unbiz.mysql.lib.protocol.tool.StringTool;

class TextColumnValueProvider implements ColumnValueProvider {
    private final byte[] binValue;
    private MyCharset charset = MyCharset.UTF8;
    private boolean isConvert = false;
    private String convertedString;

    public TextColumnValueProvider(byte[] binValue) {
        this.binValue = binValue;
    }

    @Override
    public String getValueAsString() {
        if (!isConvert) {
            if (binValue == null) {
                convertedString = null;
            }
            convertedString = StringTool.safeConvertBytes2String(binValue, charset.getCharsetName());
            isConvert = true;
        }

        return convertedString;
    }

    @Override
    public Integer getValueAsInt() {
        if (isNull()) {
            return null;
        }
        return Integer.parseInt(getValueAsString());
    }

    @Override
    public Long getValueAsLong() {
        if (isNull()) {
            return null;
        }
        return Long.parseLong(getValueAsString());
    }

    @Override
    public void useCharset(MyCharset charset) {
        this.charset = charset;
    }

    @Override
    public boolean isNull() {
        return binValue == null;
    }

}
