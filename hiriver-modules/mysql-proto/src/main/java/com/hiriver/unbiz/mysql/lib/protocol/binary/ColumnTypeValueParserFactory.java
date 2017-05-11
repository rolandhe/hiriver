package com.hiriver.unbiz.mysql.lib.protocol.binary;

import java.util.HashMap;
import java.util.Map;

import com.hiriver.unbiz.mysql.lib.ColumnType;

/**
 * 根据列的数据类型获取对应的解析器。<br>
 * <p> 为了提高性能，每种类型的解析器被缓存到内存</p>
 * 
 * @author hexiufeng
 *
 */
public class ColumnTypeValueParserFactory {
    private static final Map<ColumnType, ColumnTypeValueParser> CACHE;
    private static final ColumnTypeValueParser UNSUPPORT = new UnsupportColumnTypeValueParser();
    private static final ColumnTypeValueParser STRINGPARSER = new StringColumnTypeValueParser();

    private ColumnTypeValueParserFactory() {
    }

    static {
        CACHE = new HashMap<ColumnType, ColumnTypeValueParser>();
        for (ColumnType t : ColumnType.values()) {
            CACHE.put(t, create(t));
        }
    }

    public static ColumnTypeValueParser factory(ColumnType type) {
        ColumnTypeValueParser parser = CACHE.get(type);
        return parser == null ? UNSUPPORT : parser;
    }

    private static ColumnTypeValueParser create(ColumnType type) {
        switch (type) {
            case MYSQL_TYPE_DECIMAL:
                return null;
            case MYSQL_TYPE_TINY:
                return new IntegerColumnTypeValueParser(1);
            case MYSQL_TYPE_SHORT:
                return new IntegerColumnTypeValueParser(2);
            case MYSQL_TYPE_LONG:
                return new IntegerColumnTypeValueParser(4);
            case MYSQL_TYPE_FLOAT:
                return new FloatColumnTypeValueParser();
            case MYSQL_TYPE_DOUBLE:
                return new DoubleColumnTypeValueParser();
            case MYSQL_TYPE_NULL:
                return new NullColumnTypeValueParser();
            case MYSQL_TYPE_TIMESTAMP:
                return new TimeStampColumnTypeValueParser();
            case MYSQL_TYPE_LONGLONG:
                return new LongColumnTypeValueParser();
            case MYSQL_TYPE_INT24:
                return new IntegerColumnTypeValueParser(3);
            case MYSQL_TYPE_DATE:
                return new DateColumnTypeValueParser();
            case MYSQL_TYPE_TIME:
                return new TimeColumnTypeValueParser();
            case MYSQL_TYPE_DATETIME:
                return new DateTimeColumnTypeValueParser();
            case MYSQL_TYPE_YEAR:
                return new YearColumnTypeValueParser();
            case MYSQL_TYPE_NEWDATE:
                return new DateColumnTypeValueParser();
            case MYSQL_TYPE_VARCHAR:
                return STRINGPARSER;
            case MYSQL_TYPE_BIT:
                return new BitColumnTypeValueParser();
            case MYSQL_TYPE_TIMESTAMP2:
                return new TimeStamp2ColumnTypeValueParser();
            case MYSQL_TYPE_DATETIME2:
                return new DateTime2ColumnTypeValueParser();
            case MYSQL_TYPE_TIME2:
                return new Time2ColumnTypeValueParser();
            case MYSQL_TYPE_NEWDECIMAL:
                return new DecimalColumnTypeValueParser();
            case MYSQL_TYPE_ENUM:
                return new EnumColumnTypeValueParser();
            case MYSQL_TYPE_SET:
                return new SetColumnTypeValueParser();
            case MYSQL_TYPE_TINY_BLOB:
                return null;
            case MYSQL_TYPE_MEDIUM_BLOB:
                return null;
            case MYSQL_TYPE_LONG_BLOB:
                return null;
            case MYSQL_TYPE_BLOB:
                return new BlobColumnTypeValueParser();
            case MYSQL_TYPE_VAR_STRING:
                return STRINGPARSER;
            case MYSQL_TYPE_STRING:
                return STRINGPARSER;
            case MYSQL_TYPE_GEOMETRY:
                return null;
        }
        return null;
    }
}
