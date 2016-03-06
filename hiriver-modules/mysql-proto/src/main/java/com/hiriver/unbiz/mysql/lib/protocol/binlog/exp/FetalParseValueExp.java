package com.hiriver.unbiz.mysql.lib.protocol.binlog.exp;

/**
 * 解析columnvalue失败时抛出的异常
 * 
 * @author hexiufeng
 *
 */
public class FetalParseValueExp extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    public FetalParseValueExp(Exception e){
        super(e);
    }
}
