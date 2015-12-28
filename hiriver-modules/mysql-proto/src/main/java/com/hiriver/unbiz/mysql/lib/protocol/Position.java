package com.hiriver.unbiz.mysql.lib.protocol;

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
