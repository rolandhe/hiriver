package com.hiriver.unbiz.mysql.lib.protocol.binlog.exp;

/**
 * 读取数据超时异常。当没有新数据或者网络抖动时可能被抛出。
 * 上层应用应该捕获该异常并试图重新连接mysql
 * 
 * @author hexiufeng
 *
 */
public class ReadTimeoutExp extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

}
