package com.hiriver.unbiz.mysql.lib.protocol.binary;

import java.util.Calendar;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

/**
 * datetime 类型解析器
 * 
 * @author hexiufeng
 *
 */
public class DateTimeColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        long tmp = MysqlNumberUtils.read8Int(buf, pos);
        long d = tmp / 1000000;
        long t = tmp % 1000000;

        int year = (int) (d / 10000);
        int month = (int) ((d % 10000) / 100);
        int date = (int) (d % 100);

        int hour = (int) (t / 10000);
        int minute = (int) ((t % 10000) / 100);
        int second = (int) (t % 100);
        Calendar cal = Calendar.getInstance();
        cal.set(year, month, date, hour, minute, second);
        return cal.getTime();
    }

}
