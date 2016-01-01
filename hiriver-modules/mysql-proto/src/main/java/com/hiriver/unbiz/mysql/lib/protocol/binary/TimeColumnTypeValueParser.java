package com.hiriver.unbiz.mysql.lib.protocol.binary;

import java.sql.Time;
import java.util.Calendar;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

/**
 * time 类型解析器
 * 
 * @author hexiufeng
 *
 */
public class TimeColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MILLISECOND, 0);
        int tmp = MysqlNumberUtils.read3Int(buf, pos);
        calendar.set(0, 0, 0, tmp / 10000, (tmp % 10000) / 100, tmp % 100);
        return new Time(calendar.getTimeInMillis());
    }

}
