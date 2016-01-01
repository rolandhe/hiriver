package com.hiriverunbiz.mysql.lib.exp;

/**
 * binlog事件中不合法的有效负载异常
 * 
 * @author hexiufeng
 *
 */
public class NotExpectPayloadException extends BaseExecuteException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public NotExpectPayloadException(String message) {
        super(message);
    }

}
