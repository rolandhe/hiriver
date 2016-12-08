package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import java.util.HashMap;
import java.util.Map;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.FormatDescriptionEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.RotateEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.TableMapEvent;

/**
 * 在binlog数据同步时，当前session的全局信息存储
 * 
 * @author hexiufeng
 *
 */
public class BinlogContext {
  /**
   * 表元数据的提供者，提供表字段、类型等
   */
  private TableMetaProvider tableMetaProvider;
  /**
   * 日志翻滚事件，用于描述当前或者将要正在发送binlog文件名称及position
   */
  private RotateEvent rotateEvent;
  /**
   * 当前binlog的事件格式描述事件，在执行同步命令后，mysql server 首先会发送Rotate event，之后会发送本事件。
   * 在整个sesition中只有这一个事件，其中的eventTypeHeaderLenArray属性会被用到
   */
  private FormatDescriptionEvent foramtDescEvent;
  /**
   * 当前事务的数据包所属表的元数据描述事件，不同的事务或者不同表的数据来临时，会有不同的 TableMapEvent发送过来，在这里缓存，用于后续数据的解析
   */
  // private TableMapEvent tableMapEvent;

  private final Map<String, TableMapEvent> tableMapEventHolder = new HashMap<>();
  private final Map<Long, TableMapEvent> tableIdMapping = new HashMap<>();



  public TableMetaProvider getTableMetaProvider() {
    return tableMetaProvider;
  }

  public FormatDescriptionEvent getForamtDescEvent() {
    return foramtDescEvent;
  }

  public void putCurrentTableMapEvent(TableMapEvent tableMapEvent) {
    TableMapEvent old = tableMapEventHolder.get(tableMapEvent.getFullTableName());
    if(old != null && old.getTableId() != tableMapEvent.getTableId()){
      tableIdMapping.remove(old.getTableId());
    }
    tableMapEventHolder.put(tableMapEvent.getFullTableName(), tableMapEvent);
    tableIdMapping.put(tableMapEvent.getTableId(), tableMapEvent);
  }
  
  public TableMapEvent getTableMapEventByTableId(long tableId){
    return tableIdMapping.get(tableId);
  }
  
  public TableMapEvent getTableMapEventByFullTableName(String fullTableName){
    return tableMapEventHolder.get(fullTableName);
  }

  public void setTableMetaProvider(TableMetaProvider tableMetaProvider) {
    this.tableMetaProvider = tableMetaProvider;
  }

  public void setForamtDescEvent(FormatDescriptionEvent foramtDescEvent) {
    this.foramtDescEvent = foramtDescEvent;
  }



  public RotateEvent getRotateEvent() {
    return rotateEvent;
  }

  public void setRotateEvent(RotateEvent rotateEvent) {
    this.rotateEvent = rotateEvent;
  }
}
