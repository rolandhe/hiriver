package com.hiriver.unbiz.mysql.lib.protocol.binary;

import java.util.Calendar;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

public class DateTime2ColumnTypeValueParser implements ColumnTypeValueParser {
    private static final long DATETIMEF_INT_OFS = 0x8000000000L;

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        long val = myDatetimePackedFromBinary(buf, pos, meta);
        // int secondPart = (int)(val % (1L<<24));
        long ymdhms = val >>> 24;
        long ymd = ymdhms >>> 17;
        long ym = ymd >> 5;
        long hms = ymdhms % (1L << 17);

        int day = (int) (ymd % (1 << 5));
        int month = (int) (ym % 13);
        int year = (int) (ym / 13);

        int second = (int) (hms % (1 << 6));
        int minute = (int) ((hms >> 6) % (1 << 6));
        int hour = (int) (hms >> 12);

        Calendar cal = Calendar.getInstance();

        cal.set(year, month, day, hour, minute, second);
        return cal.getTime();
    }

    private long myDatetimePackedFromBinary(byte[] buf, Position pos, int dec) {
        long intpart = MysqlNumberUtils.readBigEdianNInt(buf, pos, 5) - DATETIMEF_INT_OFS;
        long frac = 0;
        switch (dec) {
            case 0:
            default:
                return intpart << 24;
            case 1:
            case 2:
                frac = MysqlNumberUtils.read1Int(buf, pos) * 10000;
                break;
            case 3:
            case 4:
                frac = (MysqlNumberUtils.readBigEdianNInt(buf, pos, 2) * 100);
                break;
            case 5:
            case 6:
                frac = (MysqlNumberUtils.readBigEdianNInt(buf, pos, 3));
                break;
        }
        return intpart << 24 + frac;
    }

}
