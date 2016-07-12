package com.hiriver.unbiz.mysql.lib.protocol.binlog;

/**
 * 描述gtid的事务段
 * 
 * @author hexiufeng
 *
 */
public class GtIdInterval {
    private final long start;
    private final long stop;

    public GtIdInterval(String internelString) {
        long end = 0;
        if (internelString.indexOf('-') >= 0) {
            String[] numPosArray = internelString.split("-");
            this.start = Long.parseLong(numPosArray[0]);
            end = Long.parseLong(numPosArray[1]);
        } else {
            this.start = 1L;
            end = Long.parseLong(internelString);
        }
//        if (end == 1L) {
//            end++;
//        }
        this.stop = end;
    }

    public GtIdInterval(long start, long stop) {
        this.start = start;
        this.stop = stop;
    }

    public long getStart() {
        return start;
    }

    public long getStop() {
        return stop;
    }

    public String toShortString() {
        return "" + stop;
    }
}
