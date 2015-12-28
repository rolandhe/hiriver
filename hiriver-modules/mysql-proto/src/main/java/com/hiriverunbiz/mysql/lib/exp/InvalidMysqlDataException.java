package com.hiriverunbiz.mysql.lib.exp;

/**
 * 无效的mysql数据异常，一般发生在解析响应数据时。
 * 
 * @author hexiufeng
 *
 */
public class InvalidMysqlDataException extends BaseExecuteException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public InvalidMysqlDataException(String message) {
        super(message);
    }
}
