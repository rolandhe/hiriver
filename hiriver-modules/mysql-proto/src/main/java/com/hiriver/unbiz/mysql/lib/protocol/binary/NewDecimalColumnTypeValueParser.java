package com.hiriver.unbiz.mysql.lib.protocol.binary;

import java.math.BigDecimal;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

public class NewDecimalColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        int len = getDecimalBinSzie(meta);
        byte[] decBuf = MysqlStringUtils.readFixString(buf, pos, len);
        int scale = (meta >>> 8);
        int precision = meta & 0xff;
        BigDecimal val = getDecimal(precision, scale, decBuf);
        return val;
    }

    private static final int DIG_PER_INT32 = 9;
    private static final int SIZE_OF_INT32 = 4;

    private static final int DIG_PER_DEC1 = 9;
    private static final int DIG_BASE = 1000000000;
    private static final int DIG_MAX = DIG_BASE - 1;
    private static final int[] dig2bytes = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };
    private static final int powers10[] = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };

    private static class DecimalInfo {
        private int intg;
        private int frac;
        private int intg0;
        private int frac0;
        private int intg0x;
        private int frac0x;
        private int size;

        public int getIntg() {
            return intg;
        }

        public void setIntg(int intg) {
            this.intg = intg;
        }

        public int getFrac() {
            return frac;
        }

        public void setFrac(int frac) {
            this.frac = frac;
        }

        public int getIntg0() {
            return intg0;
        }

        public void setIntg0(int intg0) {
            this.intg0 = intg0;
        }

        public int getFrac0() {
            return frac0;
        }

        public void setFrac0(int frac0) {
            this.frac0 = frac0;
        }

        public int getIntg0x() {
            return intg0x;
        }

        public void setIntg0x(int intg0x) {
            this.intg0x = intg0x;
        }

        public int getFrac0x() {
            return frac0x;
        }

        public void setFrac0x(int frac0x) {
            this.frac0x = frac0x;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }
    }

    private int getDecimalBinSzie(int metaValue) {
        int scale = (metaValue >>> 8);
        int precision = metaValue & 0xff;
        return getDecimalBinSzie(precision, scale);
    }

    private int getDecimalBinSzie(int precision, int scale) {
        DecimalInfo info = getDecimalInfo(precision, scale);
        return info.getSize();
    }

    private DecimalInfo getDecimalInfo(int precision, int scale) {
        DecimalInfo info = new DecimalInfo();
        final int intg = precision - scale;
        final int frac = scale;
        final int intg0 = intg / DIG_PER_INT32;
        final int frac0 = frac / DIG_PER_INT32;
        final int intg0x = intg - intg0 * DIG_PER_INT32;
        final int frac0x = frac - frac0 * DIG_PER_INT32;

        int binSize = intg0 * SIZE_OF_INT32 + dig2bytes[intg0x] + frac0 * SIZE_OF_INT32 + dig2bytes[frac0x];

        info.setIntg(intg);
        info.setFrac(frac);
        info.setIntg0(intg0);
        info.setFrac0(frac0);
        info.setIntg0x(intg0x);
        info.setFrac0x(frac0x);
        info.setSize(binSize);
        return info;
    }

    private int getInt16BE(byte[] buffer, final int pos) {
        return ((buffer[pos]) << 8) | (0xff & buffer[pos + 1]);
    }

    private int getInt24BE(byte[] buffer, final int pos) {
        return (buffer[pos] << 16) | ((0xff & buffer[pos + 1]) << 8) | (0xff & buffer[pos + 2]);
    }

    private int getInt32BE(byte[] buffer, final int pos) {
        return (buffer[pos] << 24) | ((0xff & buffer[pos + 1]) << 16) | ((0xff & buffer[pos + 2]) << 8)
                | (0xff & buffer[pos + 3]);
    }

    private BigDecimal getDecimal(int precision, int scale, byte[] buf) {
        DecimalInfo info = getDecimalInfo(precision, scale);
        BigDecimal decimal =
                getDecimal0(buf, 0, info.getIntg(), info.getFrac(), info.getIntg0(), info.getFrac0(), info.getIntg0x(),
                        info.getFrac0x());
        return decimal;
    }

    private BigDecimal getDecimal0(byte[] buffer, final int begin, final int intg, final int frac, final int intg0,
            final int frac0, final int intg0x, final int frac0x) {
        final int mask = ((buffer[begin] & 0x80) == 0x80) ? 0 : -1;
        int from = begin;
        /* max string length */
        final int len = ((mask != 0) ? 1 : 0) + intg // NL
                + ((frac != 0) ? 1 : 0) + frac;
        char[] buf = new char[len];
        int pos = 0;

        if (mask != 0) /* decimal sign */
            buf[pos++] = ('-');

        final byte[] d_copy = buffer;
        d_copy[begin] ^= 0x80; /* clear sign */
        int mark = pos;

        if (intg0x != 0) {
            final int i = dig2bytes[intg0x];
            int x = 0;
            switch (i) {
                case 1:
                    x = d_copy[from] /* one byte */;
                    break;
                case 2:
                    x = getInt16BE(d_copy, from);
                    break;
                case 3:
                    x = getInt24BE(d_copy, from);
                    break;
                case 4:
                    x = getInt32BE(d_copy, from);
                    break;
            }
            from += i;
            x ^= mask;
            if (x < 0 || x >= powers10[intg0x + 1]) {
                throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + powers10[intg0x + 1]);
            }
            if (x != 0 /* !digit || x != 0 */) {
                for (int j = intg0x; j > 0; j--) {
                    final int divisor = powers10[j - 1];
                    final int y = x / divisor;
                    if (mark < pos || y != 0) {
                        buf[pos++] = ((char) ('0' + y));
                    }
                    x -= y * divisor;
                }
            }
        }

        for (final int stop = from + intg0 * SIZE_OF_INT32; from < stop; from += SIZE_OF_INT32) {
            int x = getInt32BE(d_copy, from);
            x ^= mask;
            if (x < 0 || x > DIG_MAX) {
                throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + DIG_MAX);
            }
            if (x != 0) {
                if (mark < pos) {
                    for (int i = DIG_PER_DEC1; i > 0; i--) {
                        final int divisor = powers10[i - 1];
                        final int y = x / divisor;
                        buf[pos++] = ((char) ('0' + y));
                        x -= y * divisor;
                    }
                } else {
                    for (int i = DIG_PER_DEC1; i > 0; i--) {
                        final int divisor = powers10[i - 1];
                        final int y = x / divisor;
                        if (mark < pos || y != 0) {
                            buf[pos++] = ((char) ('0' + y));
                        }
                        x -= y * divisor;
                    }
                }
            } else if (mark < pos) {
                for (int i = DIG_PER_DEC1; i > 0; i--)
                    buf[pos++] = ('0');
            }
        }

        if (mark == pos)
            /* fix 0.0 problem, only '.' may cause BigDecimal parsing exception. */
            buf[pos++] = ('0');

        if (frac > 0) {
            buf[pos++] = ('.');
            mark = pos;

            for (final int stop = from + frac0 * SIZE_OF_INT32; from < stop; from += SIZE_OF_INT32) {
                int x = getInt32BE(d_copy, from);
                x ^= mask;
                if (x < 0 || x > DIG_MAX) {
                    throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + DIG_MAX);
                }
                if (x != 0) {
                    for (int i = DIG_PER_DEC1; i > 0; i--) {
                        final int divisor = powers10[i - 1];
                        final int y = x / divisor;
                        buf[pos++] = ((char) ('0' + y));
                        x -= y * divisor;
                    }
                } else {
                    for (int i = DIG_PER_DEC1; i > 0; i--)
                        buf[pos++] = ('0');
                }
            }

            if (frac0x != 0) {
                final int i = dig2bytes[frac0x];
                int x = 0;
                switch (i) {
                    case 1:
                        x = d_copy[from] /* one byte */;
                        break;
                    case 2:
                        x = getInt16BE(d_copy, from);
                        break;
                    case 3:
                        x = getInt24BE(d_copy, from);
                        break;
                    case 4:
                        x = getInt32BE(d_copy, from);
                        break;
                }
                x ^= mask;
                if (x != 0) {
                    final int dig = DIG_PER_DEC1 - frac0x;
                    x *= powers10[dig];
                    if (x < 0 || x > DIG_MAX) {
                        throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + DIG_MAX);
                    }
                    for (int j = DIG_PER_DEC1; j > dig; j--) {
                        final int divisor = powers10[j - 1];
                        final int y = x / divisor;
                        buf[pos++] = ((char) ('0' + y));
                        x -= y * divisor;
                    }
                }
            }

            if (mark == pos)
                /* make number more friendly */
                buf[pos++] = ('0');
        }

        d_copy[begin] ^= 0x80; /* restore sign */
        String decimal = String.valueOf(buf, 0, pos);
        return new BigDecimal(decimal);
    }

}
