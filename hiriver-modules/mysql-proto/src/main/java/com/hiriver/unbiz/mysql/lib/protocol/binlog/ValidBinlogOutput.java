package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.BaseRowEvent;

/**
 * 描述有效的binlog 数据 event。有效的数据evetn包括:<br>
 * 
 * <ul>
 *  <li>Insert/update/delete row event</li>
 *  <li>GTId event</li>
 * </ul>
 * 
 * @author hexiufeng
 *
 */
public class ValidBinlogOutput {
    private final BinlogEvent event;
    private final String binlogFileName;
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

    public boolean isRowEvent() {
        return eventType == ValidEventType.ROW;
    }
    public BaseRowEvent getRowEvent(){
        return (BaseRowEvent)event;
    }
    public BinlogEvent getEvent() {
        return event;
    }
    
    
}
