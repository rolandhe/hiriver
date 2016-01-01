package com.hiriver.unbiz.mysql.lib.protocol.binary;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

/**
 * mysql bigint类型的解析器，如果该字段是unsigned，返回的是BigInteger
 * 
 * 
 * @author hexiufeng
 *
 */
public class LongColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        long value = MysqlNumberUtils.read8Int(buf, pos);
        if (columnDef.isUnsigned()) {
            ByteBuffer bbf = ByteBuffer.allocate(8);
            bbf.order(ByteOrder.BIG_ENDIAN);
            bbf.putLong(value);
            return new BigInteger(bbf.array());
        } else {
            return Long.valueOf((int) value);
        }
    }

}
