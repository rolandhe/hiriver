package com.hiriver.local;

import com.hiriver.unbiz.mysql.lib.output.BinlogColumnValue;
import com.hiriver.unbiz.mysql.lib.output.BinlogResultRow;
import com.hiriver.unbiz.mysql.lib.output.RowModifyTypeEnum;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidEventType;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.BaseRowEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.GTidEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.TableMapEvent;

import java.util.List;

/**
 * Created by hexiufeng on 2017/6/19.
 */
public class DefaultLocalEventProcessor implements LocalEventProcessor {
  private String gtid = "";

  @Override
  public void processTableMapEvent(TableMapEvent event) {
    // do nothing
  }

  @Override
  public void processValidBinlogOutput(ValidBinlogOutput validBinlogOutput) {
    if (validBinlogOutput.getEventType() == ValidEventType.GTID) {
      GTidEvent gevent = (GTidEvent) validBinlogOutput.getEvent();
      gtid = gevent.getGTidString();
      return;
    }
    if (validBinlogOutput.getEventType() == ValidEventType.ROW) {
      BaseRowEvent rowEvent = (BaseRowEvent) validBinlogOutput.getEvent();
      String tableName = rowEvent.getTableMapEvent().getFullTableName();

      doTableRows(tableName,gtid,rowEvent);
      return;
    }
    if (validBinlogOutput.getEventType() == ValidEventType.TRANS_COMMIT
            || validBinlogOutput.getEventType() == ValidEventType.TRANS_ROLLBACK) {
      doTransactionEnd(gtid);
      gtid = "";
      return;
    }
  }

  protected void doTableRows(final String tableName,final String gtid,final BaseRowEvent rowEvent){
    List<BinlogResultRow> rowList = rowEvent.getRowList();
    for (BinlogResultRow row : rowList) {
      int index = 0;
      if (row.getRowModifyType() == RowModifyTypeEnum.UPDATE) {
        System.out.println("time:" + row.getBinlogOccurTime() + ",gtid:" + gtid + ",table:" + tableName + ",update");
        for (BinlogColumnValue colValue : row.getBeforeColumnValueList()) {
          BinlogColumnValue after = row.getAfterColumnValueList().get(index++);
          System.out.println(colValue.getDefinition().getColumName());
          System.out.println(colValue.getValue());
          System.out.println(after.getValue());
          System.out.println("===========================================");
        }
        continue;
      }

      List<BinlogColumnValue> valueList = null;
      if (row.getRowModifyType() == RowModifyTypeEnum.INSERT) {
        System.out.println("time:" + row.getBinlogOccurTime() + ",gtid:" + gtid + ",table:" + tableName + ",insert");
        valueList = row.getAfterColumnValueList();
      }

      if (row.getRowModifyType() == RowModifyTypeEnum.DELETE) {
        System.out.println("time:" + row.getBinlogOccurTime() + ",gtid:" + gtid + ",table:" + tableName + ",delete");
        valueList = row.getBeforeColumnValueList();
      }

      for (BinlogColumnValue colValue : valueList) {
        System.out.println(colValue.getDefinition().getColumName());
        System.out.println(colValue.getValue());
        System.out.println("===========================================");
      }
    }
  }

  protected void doTransactionEnd(final String gtid){
    // do nothing
  }
}
