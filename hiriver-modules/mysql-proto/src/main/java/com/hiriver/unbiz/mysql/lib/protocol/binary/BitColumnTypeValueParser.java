package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

public class BitColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        int nbits = ((meta >> 8) * 8) + (meta & 0xff);
        int len = (nbits + 7) / 8;
        if (nbits > 1) {
            return MysqlNumberUtils.readNInt(buf, pos, len);
        }
        return MysqlNumberUtils.read1Int(buf, pos);
    }

}
