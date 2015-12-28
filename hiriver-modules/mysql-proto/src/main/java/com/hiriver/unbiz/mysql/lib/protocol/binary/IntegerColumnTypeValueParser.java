package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

public class IntegerColumnTypeValueParser implements ColumnTypeValueParser {
    private final int len;

    public IntegerColumnTypeValueParser(int len) {
        this.len = len;
    }

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        if (columnDef.isUnsigned()) {
            long value = MysqlNumberUtils.readNInt(buf, pos, len);
            if (len < 4) {
                return Integer.valueOf((int) value);
            }
            return Long.valueOf(value);
        } else {
            int value = MysqlNumberUtils.readNRawInt(buf, pos, len);
            return Integer.valueOf((int) value);
        }
    }

}
