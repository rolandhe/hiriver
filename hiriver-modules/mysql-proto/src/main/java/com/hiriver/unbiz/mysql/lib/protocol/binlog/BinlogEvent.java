package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.Response;

/**
 * 描述binlog事件的接口。binlog eventheader中提供当前事件的binlog位置和事件写入时间，
 * 这些信息会被应用方使用，尤其是调试时，非常有用，因此Binlog事件中需要携带这些信息。
 * binlog pos和Rotate 时间的binlog file name可以唯一确定当前事件在binlog file中的位置
 * 
 * @author hexiufeng
 *
 */
public interface BinlogEvent extends Response {
    /**
     * 获取当前事件在binlog file中的位置
     * 
     * @return 当前事件在binlog file中的位置
     */
    long getBinlogEventPos();
    /**
     * 设置事件发生的时间
     * 
     * @param occurTime 事件发生的时间
     */
    void acceptOccurTime(long occurTime);
    /**
     * 获取当前事件发生的时间
     * @return
     */
    long getOccurTime();
}
