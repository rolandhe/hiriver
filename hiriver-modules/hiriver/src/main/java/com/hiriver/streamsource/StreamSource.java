package com.hiriver.streamsource;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.ReadTimeoutExp;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * mysql binlog 数据流源抽象描述。有两种实现：<br>
 * <ul>
 * <li> 纯粹的物理数据源实现 </li>
 * <li> 抽象的基于多个物理数据的HA实现，它可以按照指定的策略自由使用其中的任何数据源 </li>
 * </ul>
 * 
 * 
 * @author hexiufeng
 *
 */
public interface StreamSource {
    /**
     * 打开数据，并从指定的同步点同步数据
     * 
     * @param binlogPos 同步点
     */
    void openStream(BinlogPosition binlogPos);
    /**
     * 读取有效的事件信息
     * 
     * @return 事件信息，可能为空
     * @throws ReadTimeoutExp 可能会读取超时
     */
    ValidBinlogOutput readValidInfo() throws ReadTimeoutExp;
    /**
     * 当前数据源的host:port
     * 
     * @return
     */
    String getHostUrl();
    /**
     * 资源释放
     */
    void release();
    /**
     * 是否已经打开
     * 
     * @return 是否打开
     */
    boolean isOpen();
}
