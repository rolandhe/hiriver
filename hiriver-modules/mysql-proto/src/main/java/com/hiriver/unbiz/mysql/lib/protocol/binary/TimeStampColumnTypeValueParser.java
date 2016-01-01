package com.hiriver.unbiz.mysql.lib.protocol.binary;

import java.sql.Timestamp;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

/**
 * timestamp类型解析器
 * 
 * @author hexiufeng
 *
 */
public class TimeStampColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        long secValue = MysqlNumberUtils.readNInt(buf, pos, 4);
        secValue *=1000;
        return new  Timestamp(secValue);
    }

}
