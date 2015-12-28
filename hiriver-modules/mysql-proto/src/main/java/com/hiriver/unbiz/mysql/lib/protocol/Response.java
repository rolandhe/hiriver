package com.hiriver.unbiz.mysql.lib.protocol;

/**
 * 与mysql交互的响应对象
 * 
 * @author hexiufeng
 *
 */
public interface Response {
    /**
     * 把byte[]数据转换为响应对象
     * 
     * @param buf byte[]数组
     */
    void parse(byte[] buf);

    void parse(byte[] buf, Position pos);
}
