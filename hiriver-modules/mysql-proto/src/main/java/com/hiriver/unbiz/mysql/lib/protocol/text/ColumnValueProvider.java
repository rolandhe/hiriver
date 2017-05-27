package com.hiriver.unbiz.mysql.lib.protocol.text;


/**
 * 解析列值的提供者抽象。用于解析每列的值
 * 
 * @author hexiufeng
 *
 */
public interface ColumnValueProvider {
    /**
     * 解析成string类型
     * 
     * @return string类型
     */
    String getValueAsString();

    /**
     * 解析成Integer值
     * 
     * @return Integer值
     */
    Integer getValueAsInt();

    /**
     * 解析成long
     * 
     * @return long值
     */
    Long getValueAsLong();

    /**
     * 是否是空值
     * 
     * @return boolean
     */
    boolean isNull();

    /**
     * 当数据类型是string时，应该使用哪种charset来解析
     * 
     * @param charset
     */
    void useCharset(String charset);
}
