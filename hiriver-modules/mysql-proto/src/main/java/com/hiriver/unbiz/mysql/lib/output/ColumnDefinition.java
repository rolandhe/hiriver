package com.hiriver.unbiz.mysql.lib.output;

import com.hiriver.unbiz.mysql.lib.ColumnType;
import com.hiriver.unbiz.mysql.lib.MyCharset;

public class ColumnDefinition {
    private String columName;
    private ColumnType type;
    private MyCharset charset;
    private boolean isUnsigned;
    private boolean isPrimary;
    private boolean isUnique;
    private boolean isKey;

    public String getColumName() {
        return columName;
    }

    public ColumnType getType() {
        return type;
    }

    public void setColumName(String columName) {
        this.columName = columName;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public MyCharset getCharset() {
        return charset;
    }

    public void setCharset(MyCharset charset) {
        this.charset = charset;
    }

    public boolean isUnsigned() {
        return isUnsigned;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setUnsigned(boolean isUnsigned) {
        this.isUnsigned = isUnsigned;
    }

    public void setPrimary(boolean isPrimary) {
        this.isPrimary = isPrimary;
    }

    public void setUnique(boolean isUnique) {
        this.isUnique = isUnique;
    }

    public void setKey(boolean isKey) {
        this.isKey = isKey;
    }

}
