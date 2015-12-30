package com.hiriver.unbiz.mysql.lib.protocol;

/**
 * 抽象的mysql client-server协议的返回包
 * 
 * @author hexiufeng
 *
 */
public abstract class AbstractResponse implements Response {
    private boolean checkSum = false;
    public boolean isCheckSum() {
        return checkSum;
    }
    public void setCheckSum(boolean checkSum) {
        this.checkSum = checkSum;
    }
    @Override
    public void parse(byte[] buf, Position pos) {
        throw new RuntimeException("don't support this method.");
    }
}
