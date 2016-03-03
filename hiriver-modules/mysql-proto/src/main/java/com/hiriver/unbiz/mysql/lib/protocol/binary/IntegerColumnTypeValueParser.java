package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

/**
 * 32 bit int类型解析器，对应mysql的long，mysql的长整形是bigint，如果该字段是usigned的，返回的是java Long类型
 * 
 * @author hexiufeng
 *
 */
public class IntegerColumnTypeValueParser implements ColumnTypeValueParser {
    private final int len;

    public IntegerColumnTypeValueParser(int len) {
        this.len = len;
    }

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        if (columnDef.isUnsigned()) {
            long value = MysqlNumberUtils.readNInt(buf, pos, len);
//            if (len <= 4 && value <= Integer.MAX_VALUE) {
//                return Integer.valueOf((int) value);
//            }
            return Long.valueOf(value);
        } else {
            int value = MysqlNumberUtils.readNRawInt(buf, pos, len);
            return Integer.valueOf(value);
        }
    }

}
