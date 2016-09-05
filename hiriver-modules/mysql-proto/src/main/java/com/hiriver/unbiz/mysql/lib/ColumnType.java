package com.hiriver.unbiz.mysql.lib;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.InvalidColumnType;

/**
 * mysql所支持的列的数据类型，和java的类型有所不同
 * 
 * @author hexiufeng
 *
 */
public enum ColumnType {
    MYSQL_TYPE_DECIMAL(0x00, 2), 
    MYSQL_TYPE_TINY(0x01, 0),
    MYSQL_TYPE_SHORT(0x02, 0), 
    MYSQL_TYPE_LONG(0x03, 0), 
    MYSQL_TYPE_FLOAT(0x04, 1), 
    MYSQL_TYPE_DOUBLE(0x05, 1),
    MYSQL_TYPE_NULL(0x06, 0), 
    MYSQL_TYPE_TIMESTAMP(0x07, 0), 
    MYSQL_TYPE_LONGLONG(0x08, 0),
    MYSQL_TYPE_INT24(0x09, 0), 
    MYSQL_TYPE_DATE(0x0a, 0), 
    MYSQL_TYPE_TIME(0x0b, 0),
    MYSQL_TYPE_DATETIME(0x0c, 0), 
    MYSQL_TYPE_YEAR(0x0d, 0),
    MYSQL_TYPE_NEWDATE(0x0e, 0),
    MYSQL_TYPE_VARCHAR(0x0f, 2),
    MYSQL_TYPE_BIT(0x10, 2), 
    MYSQL_TYPE_TIMESTAMP2(0x11, 1),
    MYSQL_TYPE_DATETIME2(0x12, 1), // since mysql5.6, should be 1
    MYSQL_TYPE_TIME2(0x13, 0),
    MYSQL_TYPE_NEWDECIMAL(0xf6, 2),
    MYSQL_TYPE_ENUM(0xf7, 2), 
    MYSQL_TYPE_SET(0xf8, 2),
    MYSQL_TYPE_TINY_BLOB(0xf9, 0), 
    MYSQL_TYPE_MEDIUM_BLOB(0xfa, 0),
    MYSQL_TYPE_LONG_BLOB(0xfb, 0), 
    MYSQL_TYPE_BLOB(0xfc, 1), 
    MYSQL_TYPE_VAR_STRING(0xfd, 2),
    MYSQL_TYPE_STRING(0xfe, 2),
    MYSQL_TYPE_GEOMETRY(0xff, 0);

    private int typeValue;
    private int mataLen;
    private String name;

    private ColumnType(int typeValue, int metaLen, String name) {
        this.typeValue = typeValue;
        this.mataLen = metaLen;
        this.name = name;
    }

    public static ColumnType ofTypeValue(int typeValue){
        for(ColumnType type:ColumnType.values()){
            if(type.getTypeValue() == typeValue){
                return type;
            }
        }
        throw new InvalidColumnType("type value is :" + typeValue);
    }
    private ColumnType(int typeValue, int metaLen) {
        this(typeValue, metaLen, "");
    }

    public int getTypeValue() {
        return typeValue;
    }

    public int getMataLen() {
        return mataLen;
    }

    public String getName() {
        return name;
    }
}
