package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.BaseRowEvent;

/**
 * 描述有效的binlog 数据 event。有效的数据evetn包括:<br>
 * 
 * <ul>
 *  <li>Insert/update/delete row event</li>
 *  <li>GTId event</li>
 *  <li>XidEvent </li>
 * </ul>
 * 
 * @author hexiufeng
 *
 */
public class ValidBinlogOutput {
    /**
     * 有效事件
     */
    private final BinlogEvent event;
    /**
     * binlog文件名称，配合event中的pos可以确定当前事件在binlog file中的位置
     */
    private final String binlogFileName;
    /**
     * 当前事件的类型
     */
    private final ValidEventType eventType;

    public ValidBinlogOutput(BinlogEvent event,String binlogFileName,ValidEventType eventType) {
        this.event = event;
        this.binlogFileName = binlogFileName;
        this.eventType = eventType;
    }

    public ValidEventType getEventType() {
        return eventType;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    /**
     * 当前事件是否是行数据事件
     * 
     * @return boolean
     */
    public boolean isRowEvent() {
        return eventType == ValidEventType.ROW;
    }
    public BaseRowEvent getRowEvent(){
        return (BaseRowEvent)event;
    }
    public BinlogEvent getEvent() {
        return event;
    }
    
    public String getEventBinlogPos(){
        return binlogFileName + ":" + getEvent().getBinlogEventPos();
    }
}
