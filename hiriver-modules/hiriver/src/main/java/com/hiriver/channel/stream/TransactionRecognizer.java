package com.hiriver.channel.stream;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * 事务信息的识别器描述
 * 
 * @author hexiufeng
 *
 */
public interface TransactionRecognizer {
    /**
     * 当前接收的事件是否是事务的开始事件。Query event并且执行的sql是BEGIN则表示该事件是事务开始事件
     * 
     * @param validOutput 接收的事件
     * @return 是否是事务开始事件
     */
    boolean isStart(ValidBinlogOutput validOutput);
    /**
     * 是否是事务结束事件，当接收到的事件是Xid event时，表示它是事务结束事件
     * 
     * @param validOutput 接收的事件
     * @return 是否是事务结束事件
     */
    boolean isEnd(ValidBinlogOutput validOutput);
    /**
     * 接收的事件是否可以提前到事务的同步点
     * 
     * @param validOutput 接收的事件
     * @return 是否是事务的同步点
     */
    boolean tryRecognizePos(ValidBinlogOutput validOutput);
    /**
     * 获取当前的事务开始的同步点
     * 
     * @return 同步点
     */
    BinlogPosition getCurrentTransBeginPos();
    
    /**
     * 读取当前事务的gtid，如果mysql不支持gtid，则返回null
     * 
     * @return 当前事务的gtid
     */
    String getGTId();

    /**
     * 读取事务的binlog pos
     *
     * @return
     */
    String getTransBinlogPos();
}
