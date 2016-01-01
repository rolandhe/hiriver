package com.hiriver.unbiz.mysql.lib.protocol.binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

/**
 * double数据类型解析器
 * 
 * @author hexiufeng
 *
 */
public class DoubleColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        byte[] array = MysqlStringUtils.readFixString(buf, pos, 8);
        ByteBuffer bbf = ByteBuffer.wrap(array);
        bbf.order(ByteOrder.LITTLE_ENDIAN);
        return bbf.getDouble();
    }

}
