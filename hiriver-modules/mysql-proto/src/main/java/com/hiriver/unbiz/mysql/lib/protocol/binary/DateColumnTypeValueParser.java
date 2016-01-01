package com.hiriver.unbiz.mysql.lib.protocol.binary;

import java.util.Calendar;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

/**
 * date 类型的解析器
 * 
 * @author hexiufeng
 *
 */
public class DateColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        int tmp = MysqlNumberUtils.read3Int(buf, pos);
        int day = tmp & 31;
        int month = (tmp >> 5 & 15);
        int year = tmp >> 9;
        Calendar cal = Calendar.getInstance();
        cal.set(year, month, day, 0, 0, 0);
        return cal.getTime();
    }

}
