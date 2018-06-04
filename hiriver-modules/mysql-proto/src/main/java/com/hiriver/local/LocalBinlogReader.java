package com.hiriver.local;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiriver.unbiz.mysql.lib.ColumnType;
import com.hiriver.unbiz.mysql.lib.output.BinlogColumnValue;
import com.hiriver.unbiz.mysql.lib.output.BinlogResultRow;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.output.RowModifyTypeEnum;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogContext;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEventHeader;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.InternelColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TableMeta;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TableMetaProvider;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidEventType;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.BaseRowEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.EventFactory;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.FormatDescriptionEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.GTidEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.QueryEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.RotateEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.TableMapEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.XidEvent;

public class LocalBinlogReader {
  private static final Logger LOG = LoggerFactory.getLogger(LocalBinlogReader.class);

  private final String filePath;
  private final BinlogContext context = new BinlogContext();
  /**
   * mysql binlog使用使用校验，mysql 7默认开启校验字段
   */
  private final boolean checkSum;
  private int EVENT_HEADER_LEN = 19;

  public static class FileEndExp extends RuntimeException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public FileEndExp() {
      super();
    }
  }

  public LocalBinlogReader(String filePath,boolean checkSum) {
    this.filePath = filePath;
    this.checkSum = checkSum;
    context.setTableMetaProvider(new TableMetaProvider() {

      @Override
      public TableMeta getTableMeta(long tableId,TableMapEvent tableMapEvent) {
        TableMeta meta = new TableMeta(tableId);
        int index = 0;
        for (InternelColumnDefinition def : tableMapEvent.getColumnDefList()) {
          ColumnDefinition cdf = new ColumnDefinition();
          cdf.setType(def.getColumnType());
          cdf.setColumName("@" + index);
          cdf.setCharset("binary");
          if (def.getColumnType() == ColumnType.MYSQL_TYPE_VARCHAR
              || def.getColumnType() == ColumnType.MYSQL_TYPE_STRING
              || def.getColumnType() == ColumnType.MYSQL_TYPE_VAR_STRING
              || def.getColumnType() == ColumnType.MYSQL_TYPE_BLOB
              || def.getColumnType() == ColumnType.MYSQL_TYPE_LONG_BLOB
              || def.getColumnType() == ColumnType.MYSQL_TYPE_MEDIUM_BLOB) {
            cdf.setCharset("UTF-8");
          }
          meta.addColumn(cdf);
          index++;
        }

        return meta;
      }

    });
  }


  public void traversal(final LocalEventProcessor eventProcessor) {
    FileInputStream fs = null;

    try {
      fs = new FileInputStream(filePath);
      readHeader(fs);
      readMeta(fs);
      readValidEvent(fs,eventProcessor);
    } catch (FileNotFoundException e) {
      LOG.error("open file error.", e);
    } catch (IOException e) {
      LOG.error("read error.", e);

    } catch (FileEndExp e) {
      // LOG.info("read to end");
    } catch (RuntimeException e) {

      LOG.error("traversal error.", e);
    } finally {
      if (fs != null) {
        try {
          fs.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
  }

  private void readValidEvent(final FileInputStream fs,final LocalEventProcessor eventProcessor) throws IOException {

    while (true) {
      BinlogEvent event = readEvent(fs);
      if (event instanceof TableMapEvent) {
        context.putCurrentTableMapEvent((TableMapEvent) event);
        eventProcessor.processTableMapEvent((TableMapEvent) event);
        continue;
      }

      ValidBinlogOutput out = distinguishEvent(event);
      if (out == null) {
        continue;
      }
      eventProcessor.processValidBinlogOutput(out);
    }
  }


  private ValidBinlogOutput distinguishEvent(BinlogEvent event) {
    if (event instanceof GTidEvent) {
      return new ValidBinlogOutput(event, filePath, ValidEventType.GTID);
    }
    if (event instanceof BaseRowEvent) {
      return new ValidBinlogOutput(event, filePath, ValidEventType.ROW);
    }
    if (event instanceof XidEvent) {
      return new ValidBinlogOutput(event, filePath, ValidEventType.TRANS_COMMIT);
    }
    if (event instanceof QueryEvent) {
      QueryEvent qEvent = (QueryEvent) event;
      if ("BEGIN".equals(qEvent.getQuery())) {
        return new ValidBinlogOutput(event, filePath, ValidEventType.TRAN_BEGIN);
      }
      if ("ROLLBACK".equals(qEvent.getQuery())) {
        return new ValidBinlogOutput(event, filePath, ValidEventType.TRANS_ROLLBACK);
      }
      return null;
    }

    return null;
  }

  private void readMeta(final FileInputStream fs) throws IOException {
    while (true) {
      BinlogEvent event = readEvent(fs);
      if (event instanceof RotateEvent) {
        context.setRotateEvent((RotateEvent) event);
        continue;
      }
      if (event instanceof FormatDescriptionEvent) {
        context.setForamtDescEvent((FormatDescriptionEvent) event);
        break;
      }
    }
  }

  private BinlogEvent readEvent(final FileInputStream fs) throws IOException {
    byte[] buf = readByLen(fs, EVENT_HEADER_LEN);
    BinlogEventHeader eventHeader = new BinlogEventHeader();

    eventHeader.parse(buf, Position.factory());
    BinlogEvent event = EventFactory.factory(eventHeader.getEventType(), eventHeader.getLogPos(), context, checkSum);
    Position pos = Position.factory();
    byte[] eventPayload = readByLen(fs, eventHeader.getRestContentLen());
    if (event instanceof BaseRowEvent) {
      BaseRowEvent rowEvent = (BaseRowEvent) event;
      rowEvent.parseTableId(eventPayload, pos);
    }
    event.acceptOccurTime(eventHeader.getTimestamp());
    event.parse(eventPayload, pos);
    return event;
  }

  private void readHeader(final FileInputStream fs) throws IOException {
    int magic = fs.read() & 0xff;
    if (magic != 0xfe) {
      throw new RuntimeException("first byte must be 0xfe");
    }
    byte[] bin = new byte[3];
    int len = fs.read(bin);
    if (len != 3 || !"bin".equals(new String(bin))) {
      throw new RuntimeException("it is not a mysql binlog file.");
    }
  }

  private byte[] readByLen(final FileInputStream fs, int len) throws IOException {
    byte[] buf = new byte[len];
    int readLen = fs.read(buf);
    if (readLen == -1) {
      throw new FileEndExp();
    }
    if (readLen != len) {
      throw new RuntimeException("file is end, it needs more bytes");
    }
    return buf;
  }


  public static void main(String[] args) {
    long start = System.currentTimeMillis();
    for (String path : args) {
      LocalBinlogReader reader = new LocalBinlogReader(path, true);
      reader.traversal(new DefaultLocalEventProcessor());
    }
    System.out.println(System.currentTimeMillis() - start);
  }
}
