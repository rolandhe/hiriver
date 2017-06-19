package com.hiriver.local;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.TableMapEvent;

/**
 * Created by hexiufeng on 2017/6/19.
 */
public interface LocalEventProcessor {
  void processTableMapEvent(TableMapEvent event);
  void processValidBinlogOutput(ValidBinlogOutput validBinlogOutput);
}
