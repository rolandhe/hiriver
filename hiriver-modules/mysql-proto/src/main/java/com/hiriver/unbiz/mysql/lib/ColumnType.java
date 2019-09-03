package com.hiriver.unbiz.mysql.lib;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.InvalidColumnType;
import org.apache.commons.lang3.StringUtils;

/**
 * mysql所支持的列的数据类型，和java的类型有所不同
 * 
 * @author hexiufeng
 *
 */
public enum ColumnType {
    MYSQL_TYPE_DECIMAL(0x00, 2,new String[]{"decimal"}),
    MYSQL_TYPE_TINY(0x01, 0,new String[]{"tinyint"}),
    MYSQL_TYPE_SHORT(0x02, 0,new String[]{"smallint"}),
    MYSQL_TYPE_LONG(0x03, 0,new String[]{"int"}),
    MYSQL_TYPE_FLOAT(0x04, 1,new String[]{"float"}),
    MYSQL_TYPE_DOUBLE(0x05, 1,new String[]{"double"}),
    MYSQL_TYPE_NULL(0x06, 0,new String[]{}),
    MYSQL_TYPE_TIMESTAMP(0x07, 0,new String[]{}),
    MYSQL_TYPE_LONGLONG(0x08, 0,new String[]{"bigint"}),
    MYSQL_TYPE_INT24(0x09, 0,new String[]{"int24"}),
    MYSQL_TYPE_DATE(0x0a, 0,new String[]{}),
    MYSQL_TYPE_TIME(0x0b, 0,new String[]{}),
    MYSQL_TYPE_DATETIME(0x0c, 0,new String[]{}),
    MYSQL_TYPE_YEAR(0x0d, 0,new String[]{"year"}),
    MYSQL_TYPE_NEWDATE(0x0e, 0,new String[]{"date"}),
    MYSQL_TYPE_VARCHAR(0x0f, 2,new String[]{"verchar"}),
    MYSQL_TYPE_BIT(0x10, 2,new String[]{"bit"}),
    MYSQL_TYPE_TIMESTAMP2(0x11, 1,new String[]{"timestamp"}),
    MYSQL_TYPE_DATETIME2(0x12, 1,new String[]{"datetime"}), // since mysql5.6, should be 1
    MYSQL_TYPE_TIME2(0x13, 1,new String[]{"time"}),
    MYSQL_TYPE_NEWDECIMAL(0xf6, 2,new String[]{"decimal"}),
    MYSQL_TYPE_ENUM(0xf7, 2,new String[]{"enum"}),
    MYSQL_TYPE_SET(0xf8, 2,new String[]{"set"}),
    MYSQL_TYPE_TINY_BLOB(0xf9, 0,new String[]{"tinyblob"}),
    MYSQL_TYPE_MEDIUM_BLOB(0xfa, 0,new String[]{"mediumblob"}),
    MYSQL_TYPE_LONG_BLOB(0xfb, 0,new String[]{"longblob"}),
    MYSQL_TYPE_BLOB(0xfc, 1,new String[]{"blob","text"}),
    MYSQL_TYPE_VAR_STRING(0xfd, 2,new String[]{"varstring"}),
    MYSQL_TYPE_STRING(0xfe, 2,new String[]{"string"}),
    MYSQL_TYPE_GEOMETRY(0xff, 0,new String[]{"geometry"});

    private int typeValue;
    private int mataLen;
    private String[] names;

    private ColumnType(int typeValue, int metaLen, String[] names) {
        this.typeValue = typeValue;
        this.mataLen = metaLen;
        this.names = names;
    }

    public static ColumnType ofTypeValue(int typeValue){
        for(ColumnType type:ColumnType.values()){
            if(type.getTypeValue() == typeValue){
                return type;
            }
        }
        throw new InvalidColumnType("type value is :" + typeValue);
    }

    public static ColumnType ofTypeName(String name){
        for(ColumnType type:ColumnType.values()){
            for(String n:type.names) {
                if(n.equalsIgnoreCase(name)) {
                    return type;
                }
            }
//           String[] mulNames =  StringUtils.split(type.name,",");
//           for(String n : mulNames) {
//               if(n.equalsIgnoreCase(name)) {
//                   return type;
//               }
//           }
        }
//        throw new InvalidColumnType("type name is :" + name);
        return null;
    }

    private ColumnType(int typeValue, int metaLen) {
        this(typeValue, metaLen, new String[]{});
    }

    public int getTypeValue() {
        return typeValue;
    }

    public int getMataLen() {
        return mataLen;
    }

}
