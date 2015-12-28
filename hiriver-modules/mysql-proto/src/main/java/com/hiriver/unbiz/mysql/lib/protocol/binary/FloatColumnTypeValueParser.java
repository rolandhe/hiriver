package com.hiriver.unbiz.mysql.lib.protocol.binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

public class FloatColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        byte[] array = MysqlStringUtils.readFixString(buf, pos, 4);
        ByteBuffer bbf = ByteBuffer.wrap(array);
        bbf.order(ByteOrder.LITTLE_ENDIAN);
        return bbf.getFloat();
    }

}
