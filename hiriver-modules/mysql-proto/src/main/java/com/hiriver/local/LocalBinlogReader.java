package com.hiriver.local;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiriver.unbiz.mysql.lib.ColumnType;
import com.hiriver.unbiz.mysql.lib.MyCharset;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
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
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.RowEventV1;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.TableMapEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.XidEvent;

public class LocalBinlogReader {
    private static final Logger LOG = LoggerFactory.getLogger(LocalBinlogReader.class);

    private final String filePath;
    private final BinlogContext context = new BinlogContext();
    private boolean checkSum = false;
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

    public LocalBinlogReader(String filePath) {
        this.filePath = filePath;
        context.setTableMetaProvider(new TableMetaProvider() {

            @Override
            public TableMeta getTableMeta(long tableId, String schemaName, String tableName) {
                TableMeta meta = new TableMeta(tableId);
                TableMapEvent tme = context.getTableMapEvent();
                int index = 0;
                for (InternelColumnDefinition def : tme.getColumnDefList()) {
                    ColumnDefinition cdf = new ColumnDefinition();
                    cdf.setType(def.getColumnType());
                    cdf.setColumName("@" + index);
                    cdf.setCharset(MyCharset.BINARY);
                    if (def.getColumnType() == ColumnType.MYSQL_TYPE_VARCHAR
                            || def.getColumnType() == ColumnType.MYSQL_TYPE_STRING
                            || def.getColumnType() == ColumnType.MYSQL_TYPE_VAR_STRING
                            || def.getColumnType() == ColumnType.MYSQL_TYPE_BLOB
                            || def.getColumnType() == ColumnType.MYSQL_TYPE_LONG_BLOB
                            || def.getColumnType() == ColumnType.MYSQL_TYPE_MEDIUM_BLOB) {
                        cdf.setCharset(MyCharset.UTF8);
                    }
                    meta.addColumn(cdf);
                    index++;
                }

                return meta;
            }

        });
    }

    public void traversal() {
        FileInputStream fs = null;

        try {
            fs = new FileInputStream(filePath);
            readHeader(fs);
            readMeta(fs);
            readValidEvent(fs);
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

    private void readValidEvent(final FileInputStream fs) throws IOException {
        String gtid = "";
        int wordCount = 0;
        int allCount = 0;
        while (true) {
            BinlogEvent event = readEvent(fs);
            if (event.getBinlogEventPos() == 509054727L) {
                System.out.println("hehe");
            }

            if (event instanceof TableMapEvent) {
                context.setTableMapEvent((TableMapEvent) event);
                // if( context.getTableMapEvent().getTableName().startsWith("huichuan_ad.")){
                // System.out.println("gogo");
                // }
                // LOG.info("table name from table map event {}", context.getTableMapEvent().getTableName());
                continue;
            }
            if (event instanceof BaseRowEvent) {
                // if(!context.getTableMapEvent().getTableName().startsWith("huichuan_ad.")){
                // LOG.info("filter table event {}",context.getTableMapEvent().getTableName());
                // }
            }

            ValidBinlogOutput out = distinguishEvent(event);
            if (out == null) {
                continue;
            }
            if (out.getEventType() == ValidEventType.GTID) {
                GTidEvent gevent = (GTidEvent) out.getEvent();
                gtid = gevent.getGTidString();
                continue;
            }
            if (out.getEventType() == ValidEventType.ROW) {
                BaseRowEvent rowEvent = (BaseRowEvent) out.getEvent();
                String tableName = rowEvent.getTableMapEvent().getFullTableName();
                if(tableName.indexOf("sentinel") > 0){
                    System.out.println(rowEvent.getRowList().get(0).getAfterColumnValueList());
                }
                if (tableName.startsWith("wolong_0003.tb_winfo_") && tableName.indexOf("stat") == -1) {
                    wordCount++;
                }
                allCount++;
                continue;
            }
            if (out.getEventType() == ValidEventType.TRANS_COMMIT) {

                // if (count > 0) {
                System.out.println(
                        gtid + " " + wordCount + " " + allCount + " commit" + " " + out.getEvent().getOccurTime());
                // }
                gtid = "";
                allCount = 0;
                wordCount = 0;
                continue;
            }
            if (out.getEventType() == ValidEventType.TRANS_ROLLBACK) {
                System.out.println(gtid + "$rollback ");
                gtid = "";
                allCount = 0;
                wordCount = 0;
                continue;
            }
            // output(out);
        }
    }

    private static long count = 0;

    private void output(ValidBinlogOutput out) {
        if (out == null) {
            return;
        }

        if (out.getEventType() == ValidEventType.GTID) {
            GTidEvent gevent = (GTidEvent) out.getEvent();
            count++;
            LOG.info("gtid is {}", gevent.getGTidString());
            if ("f5d8d81a-aed5-11e5-819a-70e28411a2a1:659585576".equals(gevent.getGTidString())) {
                System.out.println("hehe");
            }
        } else if (out.getEventType() == ValidEventType.ROW) {
            BaseRowEvent rowEvent = (BaseRowEvent) out.getEvent();
            // LOG.info("row data table name is {}", rowEvent.getFullTableName());

        } else {
            // LOG.info("event type is {}", out.getEventType());
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
        BinlogEvent event =
                EventFactory.factory(eventHeader.getEventType(), eventHeader.getLogPos(), context, checkSum);

        byte[] eventPayload = readByLen(fs, eventHeader.getRestContentLen());
        event.parse(eventPayload, Position.factory());
        event.acceptOccurTime(eventHeader.getTimestamp());
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

    // private byte[] readResponsePayload(final FileInputStream fs) throws IOException{
    // readEventHeader(fs);
    // return readByLen(fs,this.header.getPayloadLen());
    // }

    public static void main(String[] args) {
//        String path = args[0];
//        if (path == null || path.length() == 0) {
//            System.out.println("please specify binlog file.");
//            return;
//        }
        long start = System.currentTimeMillis();
        for (String path : args) {
            LocalBinlogReader reader = new LocalBinlogReader(path);
            reader.traversal();
        }
        System.out.println(System.currentTimeMillis() - start);
        // System.out.println("all gtid:" + count);
    }
}
