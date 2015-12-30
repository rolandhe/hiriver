package com.hiriver.unbiz.mysql.lib;

/**
 * 当从主从复制的socket流中读取数据超时时的处理抽象，缺省实现直接抛出异常
 * 
 * @author hexiufeng
 *
 */
public interface SocketReadTimeoutHanlder {
    /**
     * 异常处理
     * 
     * @param message 辅助消息
     * @param e 异常
     */
    void handle(String message, Exception e);
}
