package com.hiriver.unbiz.mysql.lib.protocol.binlog.exp;

/**
 * 无效的列类型异常，当不能识别列类型时抛出该异常
 * 
 * @author hexiufeng
 *
 */
public class InvalidColumnType extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public InvalidColumnType(String message) {
        super(message);
    }

}
