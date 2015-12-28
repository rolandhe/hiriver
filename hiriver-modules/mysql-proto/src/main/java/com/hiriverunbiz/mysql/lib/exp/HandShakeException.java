package com.hiriverunbiz.mysql.lib.exp;

/**
 * 与mysql进行连接握手时导致的异常.
 * 
 * @author hexiufeng
 *
 */
public class HandShakeException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public HandShakeException(String message) {
        super(message);
    }
}
