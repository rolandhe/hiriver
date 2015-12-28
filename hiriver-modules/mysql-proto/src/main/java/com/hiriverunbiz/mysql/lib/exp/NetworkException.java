package com.hiriverunbiz.mysql.lib.exp;

/**
 * 底层socket错误,可能网络中断导致
 * 
 * @author hexiufeng
 *
 */
public class NetworkException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public NetworkException(String message) {
        super(message);
    }

    public NetworkException(Exception e) {
        super(e);
    }

    public NetworkException(String message, Exception e) {
        super(message, e);
    }
}
