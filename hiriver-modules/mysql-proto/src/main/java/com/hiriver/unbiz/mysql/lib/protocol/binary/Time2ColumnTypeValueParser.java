package com.hiriver.unbiz.mysql.lib.protocol.binary;

import java.util.Calendar;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

public class Time2ColumnTypeValueParser implements ColumnTypeValueParser {
    private static final long TIMEF_INT_OFS = 0x800000L;
    private static final long TIMEF_OFS = 0x800000000000L;

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {

        long tmp = myTimePackedFromBinary(buf, pos, meta);

        long hms;
        if (tmp < 0) {
            tmp = -tmp;
        }
        hms = tmp >>> 24;
        int year = 0;
        int month = 0;
        int day = 0;
        int hour = (int) (hms >>> 12) % (1 << 10); /* 10 bits starting at 12th */
        int minute = (int) (hms >>> 6) % (1 << 6); /* 6 bits starting at 6th */
        int second = (int) hms % (1 << 6); /* 6 bits starting at 0th */
        // int second_part= (int) (tmp%(1<<24));
        Calendar cal = Calendar.getInstance();

        cal.set(year, month, day, hour, minute, second);
        return cal.getTime();
    }

    private long myTimePackedFromBinary(byte[] buf, Position pos, int dec) {
        switch (dec) {
            case 0:
            default: {
                long intpart = MysqlNumberUtils.readBigEdianNInt(buf, pos, 3) - TIMEF_INT_OFS;
                return intpart << 24;
            }
            case 1:
            case 2: {
                long intpart = MysqlNumberUtils.readBigEdianNInt(buf, pos, 3) - TIMEF_INT_OFS;
                int frac = MysqlNumberUtils.read1Int(buf, pos);
                if (intpart < 0 && frac > 0) {
                    /*
                     * Negative values are stored with reverse fractional part order, for binary sort compatibility.
                     * 
                     * Disk value intpart frac Time value Memory value 800000.00 0 0 00:00:00.00 0000000000.000000
                     * 7FFFFF.FF -1 255 -00:00:00.01 FFFFFFFFFF.FFD8F0 7FFFFF.9D -1 99 -00:00:00.99 FFFFFFFFFF.F0E4D0
                     * 7FFFFF.00 -1 0 -00:00:01.00 FFFFFFFFFF.000000 7FFFFE.FF -1 255 -00:00:01.01 FFFFFFFFFE.FFD8F0
                     * 7FFFFE.F6 -2 246 -00:00:01.10 FFFFFFFFFE.FE7960
                     * 
                     * Formula to convert fractional part from disk format (now stored in "frac" variable) to absolute
                     * value: "0x100 - frac". To reconstruct in-memory value, we shift to the next integer value and
                     * then substruct fractional part.
                     */
                    intpart++; /* Shift to the next integer value */
                    frac -= 0x100; /* -(0x100 - frac) */
                }
                return intpart << 24 + frac * 10000L;
            }

            case 3:
            case 4: {
                long intpart = MysqlNumberUtils.readBigEdianNInt(buf, pos, 3) - TIMEF_INT_OFS;
                int frac = MysqlNumberUtils.read2Int(buf, pos);
                ;
                if (intpart < 0 && frac > 0) {
                    /*
                     * Fix reverse fractional part order: "0x10000 - frac". See comments for FSP=1 and FSP=2 above.
                     */
                    intpart++; /* Shift to the next integer value */
                    frac -= 0x10000; /* -(0x10000-frac) */
                }
                return intpart << 24 + frac * 100L;
            }

            case 5:
            case 6:
                return MysqlNumberUtils.readBigEdianNInt(buf, pos, 6) - TIMEF_OFS;
        }
    }
}
