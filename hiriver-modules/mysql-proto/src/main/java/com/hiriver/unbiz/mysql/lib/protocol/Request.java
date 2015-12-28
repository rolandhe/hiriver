package com.hiriver.unbiz.mysql.lib.protocol;

/**
 * 与mysql交互的请求.该请求对象包含请求头和请求主体数据
 * 
 * @author hexiufeng
 *
 */
public interface Request {
    /**
     * 把请求对象转换为可以socket发送的byte数组
     * 
     * @return byte[]数组
     */
    byte[] toByteArray();
}
