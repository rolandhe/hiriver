package com.hiriver.streamsource;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.ReadTimeoutExp;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * mysql binlog 数据流源抽象描述
 * 
 * @author hexiufeng
 *
 */
public interface StreamSource {
    void openStream(BinlogPosition binlogPos);
    ValidBinlogOutput readValidInfo() throws ReadTimeoutExp;
    String getHostUrl();
    void release();
    boolean isOpen();
}
