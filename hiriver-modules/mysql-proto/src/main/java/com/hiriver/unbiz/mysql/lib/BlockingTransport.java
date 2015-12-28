package com.hiriver.unbiz.mysql.lib;

import com.hiriver.unbiz.mysql.lib.protocol.Request;
import com.hiriverunbiz.mysql.lib.exp.HandShakeException;
import com.hiriverunbiz.mysql.lib.exp.NetworkException;
import com.hiriverunbiz.mysql.lib.exp.PeerResetNetworkException;
import com.hiriverunbiz.mysql.lib.exp.UnOpenedSocket;

/**
 * 描述blocking模型的mysql连接.它是一个客户端和mysql服务器之间连接的抽象。它是非线程安全的。
 * <p>
 * 为了和jdbc的connection区别，这里用Transport进行命名。
 * </p>
 * 
 * @author hexiufeng
 * 
 */
public interface BlockingTransport {

    /**
     * 打开连接
     * 
     * @throws NetworkException 网络异常
     * @throws HandShakeException 握手阶段发生异常
     */
    void open() throws NetworkException, HandShakeException;

    /**
     * 关闭连接
     */
    void close();

    /**
     * 连接是否已经打开
     * 
     * @return true or false
     */
    boolean isOpen();

    /**
     * ping msyql server,验证mysql服务是否存活
     * 
     * @return true or false
     */
    boolean ping();

    /**
     * 从连接中读取一个<b>有效的响应数据块</b>。有效的响应数据块指的是不包含包头的响应数据。本方法执行两个步骤：
     * <ul>
     * <li>读取包头，解析读取有效数据块的长度</li>
     * <li>读取指定长度的数据库</li>
     * </ul>
     * 
     * @return 有效数据块的数组
     * @throws NetworkException 网络发生异常
     * @throws PeerResetNetworkException 底层socket被对方关闭异常
     * @throws UnOpenedSocket 连接问未建立
     */
    byte[] readResponsePayload() throws NetworkException, PeerResetNetworkException, UnOpenedSocket;

    /**
     * 发送请求
     * 
     * @param request 请求对象
     * @throws NetworkException 网络发生异常
     * @throws UnOpenedSocket 连接问未建立
     */
    void writeRequest(Request request) throws NetworkException, UnOpenedSocket;
}
