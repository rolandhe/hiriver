package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.Response;

/**
 * 抽象描述同步binlog的返回数据包
 * 
 * @author hexiufeng
 *
 */
public abstract class AbstractBinlogResponse implements Response {

    @Override
    public void parse(byte[] buf) {
        throw new RuntimeException("don't support this method.");
    }
}
