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

    /**
     * 解析当前buf的数据，使用pos来控制当已经处理数据的位置,在数据被多次处理时非常
     * 有效，比如在解析多行数据时
     * 
     * @param buf 已经从mysql server读取到数据缓存
     * @param pos 保存当前已经处理的位置
     */
    void parse(byte[] buf, Position pos);
}
