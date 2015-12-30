package com.hiriver.unbiz.mysql.lib.protocol;

 /**
  * 从byte数组中解析数据时，描述当前解析到哪个字节的辅助类，
  * 在一包数据被持续解析时非常有用，比如解析多行数据时，
  * 
  * @author hexiufeng
  *
  */
public final class Position {
    private int pos;

    private Position(int pos) {
        this.pos = pos;
    }

    public static Position factory() {
        return factory(0);
    }

    public static Position factory(int pos) {
        return new Position(pos);
    }

    public int getPos() {
        return this.pos;
    }

    public int getAndForwordPos() {
        return pos++;
    }

    public int getAndForwordPos(int step) {
        int cur = pos;
        pos += step;
        return cur;
    }

    public int forwardPos() {
        return ++pos;
    }

    public int forwardPos(int step) {
        pos += step;
        return pos;
    }

    public void reset() {
        pos = 0;
    }

    public void reset(int newPos) {
        pos = newPos;
    }
}
