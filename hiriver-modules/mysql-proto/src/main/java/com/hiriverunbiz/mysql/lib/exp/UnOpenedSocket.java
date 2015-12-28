package com.hiriverunbiz.mysql.lib.exp;

/**
 * socket还没有别打开异常。一般是socket还没有被打开就执行读写操作引起的。
 * 
 * @author hexiufeng
 * 
 */
public class UnOpenedSocket extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public UnOpenedSocket(String message) {
        super(message);
    }

}
