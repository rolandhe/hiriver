package com.hiriver.unbiz.mysql.lib.protocol.binary;

import java.sql.Timestamp;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

/**
 * timestamp2类型解析器
 * 
 * @author hexiufeng
 *
 */
public class TimeStamp2ColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        long secValue = MysqlNumberUtils.readBigEdianNInt(buf, pos, 4);
        long usecValue = 0;
        switch (meta) {
            case 0:
            default:
                break;
            case 1:
            case 2:
                usecValue = MysqlNumberUtils.read1Int(buf, pos) * 10000L;
                break;
            case 3:
            case 4:
                usecValue = MysqlNumberUtils.readBigEdianNInt(buf, pos, 2) * 100;
                break;
            case 5:
            case 6:
                usecValue = MysqlNumberUtils.readBigEdianNInt(buf, pos, 3);
        }
        Timestamp tsValue = new Timestamp(secValue*1000);
        tsValue.setNanos((int) usecValue);
        return tsValue;
    }
}
