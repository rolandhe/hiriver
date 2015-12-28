package com.hiriverunbiz.mysql.lib.exp;

/**
 * 与mysql成功连接后，执行交互命令时发生的异常，它只是描述交互异常，但不包括网络异常s。
 * 
 * @author hexiufeng
 *
 */
public class BaseExecuteException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public BaseExecuteException(String message) {
        super(message);
    }

    public BaseExecuteException(Exception e) {
        super(e);
    }

    public BaseExecuteException(String message, Exception e) {
        super(message, e);
    }
}
