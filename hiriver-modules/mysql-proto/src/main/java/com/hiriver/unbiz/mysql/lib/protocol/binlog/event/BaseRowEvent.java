package com.hiriver.unbiz.mysql.lib.protocol.binlog.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiriver.unbiz.mysql.lib.ColumnType;
import com.hiriver.unbiz.mysql.lib.output.BinlogColumnValue;
import com.hiriver.unbiz.mysql.lib.output.BinlogResultRow;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.output.RowModifyTypeEnum;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.binary.ColumnTypeValueParserFactory;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEventType;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TableMeta;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TableMetaProvider;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.FetalParseValueExp;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.InvalidColumnType;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.TableAlreadyModifyExp;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

/**
 * 基础的、抽象的mysql binlog row事件，用于解析出具体的数据
 * 
 * @author hexiufeng
 *
 */
public abstract class BaseRowEvent extends AbstractBinlogEvent implements BinlogEvent {
    private static final Logger LOG = LoggerFactory.getLogger(BaseRowEvent.class);

    protected final int eventType;
    /**
     * 该事件所对应表的元数据描述，mysql server发送row事件数据之前要先发送该事件
     */
    protected final TableMapEvent tableMapEvent;
    protected final TableMetaProvider tableMetaProvider;
    protected final List<BinlogResultRow> rowList = new LinkedList<BinlogResultRow>();

    private long tableId;
    private int columnCount;
    private byte[] columnsNotNullBitmap;

    public String getFullTableName() {
        return tableMapEvent.getFullTableName();
    }

    public List<ColumnDefinition> getColumnDefinitionList() {
        return tableMetaProvider.getTableMeta(tableId, tableMapEvent.getSchema(), tableMapEvent.getTableName())
                .getColumnMetaList();
    }

    protected BaseRowEvent(TableMapEvent tableMapEvent, TableMetaProvider tableMetaProvider, int eventType,
            final long eventBinlogPos, boolean hasCheckSum) {
        super(eventBinlogPos, hasCheckSum);
        this.tableMapEvent = tableMapEvent;
        this.tableMetaProvider = tableMetaProvider;
        this.eventType = eventType;
    }

    /**
     * 解析子事件头数据
     * 
     * @param buf 已经读取到的事件数据
     * @param pos 位置控制器
     */
    protected abstract void parseVerPostHeader(byte[] buf, Position pos);

    /**
     * 解析update事件的主数据，不同的版本有不同的解析方式
     * 
     * @param buf 已经读取到的事件数据
     * @param pos 位置控制器
     */
    protected abstract void parseVerBodyForUpdate(byte[] buf, Position pos);

    /**
     * 解析update事件的修改之前的数据，不同的版本有不同的解析方式
     * 
     * @param buf 已经读取到的事件数据
     * @param tableMeta
     * @return
     */
    protected abstract List<BinlogColumnValue> parseVerRowForUpdate(byte[] buf, Position pos, TableMeta tableMeta);

    protected final boolean isUpdate() {
        return this.eventType == BinlogEventType.UPDATE_ROWS_EVENTv0
                || this.eventType == BinlogEventType.UPDATE_ROWS_EVENTv1
                || this.eventType == BinlogEventType.UPDATE_ROWS_EVENTv2;
    }

    protected final boolean isInsert() {
        return this.eventType == BinlogEventType.WRITE_ROWS_EVENTv0
                || this.eventType == BinlogEventType.WRITE_ROWS_EVENTv1
                || this.eventType == BinlogEventType.WRITE_ROWS_EVENTv2;
    }

    protected final boolean isDelete() {
        return this.eventType == BinlogEventType.DELETE_ROWS_EVENTv0
                || this.eventType == BinlogEventType.DELETE_ROWS_EVENTv1
                || this.eventType == BinlogEventType.DELETE_ROWS_EVENTv2;
    }

    @Override
    public void parse(byte[] buf, Position pos) {
        if (tableMapEvent.getFormatDescriptionEvent().getPostHeaderLen(eventType) == 6) {
            tableId = MysqlNumberUtils.readNInt(buf, pos, 4);
        } else {
            tableId = MysqlNumberUtils.read6Int(buf, pos);
        }

        pos.forwardPos(2);

        parseVerPostHeader(buf, pos);
        columnCount = (int) MysqlNumberUtils.readLencodeLong(buf, pos);
        if (this.tableMapEvent.getColumnCount() < columnCount) {
            LOG.error("db {},table {}, binlog column count is {},but current table column is {} ",
                    tableMapEvent.getSchema(), tableMapEvent.getTableName(), columnCount,
                    tableMapEvent.getColumnCount());

            throw new TableAlreadyModifyExp(tableMapEvent.getSchema() + "." + tableMapEvent.getTableName());
        }
        columnsNotNullBitmap = readNotNullBitmap(buf, pos);
        if (isUpdate()) {
            parseVerBodyForUpdate(buf, pos);
        }
        TableMeta tableMeta =
                tableMetaProvider.getTableMeta(tableId, tableMapEvent.getSchema(), tableMapEvent.getTableName());
        List<BinlogColumnValue> nullList = Collections.emptyList();
        int maxLen = buf.length;
        if (super.isHasCheckSum()) {
            maxLen -= 4;
        }
        while (pos.getPos() < maxLen) {
            if (isUpdate()) {
                rowList.add(new BinlogResultRow(parseRow(columnsNotNullBitmap, buf, pos, tableMeta),
                        parseVerRowForUpdate(buf, pos, tableMeta), RowModifyTypeEnum.UPDATE));
            }
            if (isInsert()) {
                rowList.add(new BinlogResultRow(nullList, parseRow(columnsNotNullBitmap, buf, pos, tableMeta),
                        RowModifyTypeEnum.INSERT));
            }
            if (isDelete()) {
                rowList.add(new BinlogResultRow(parseRow(columnsNotNullBitmap, buf, pos, tableMeta), nullList,
                        RowModifyTypeEnum.DELETE));
            }

        }
        if (super.isHasCheckSum()) {
            pos.forwardPos(4);
        }
    }

    protected final byte[] readNotNullBitmap(byte[] buf, Position pos) {
        return MysqlStringUtils.readFixString(buf, pos, calculateBytesLen(columnCount));
    }

    protected final List<BinlogColumnValue> parseRow(byte[] colsNotNullBitmap, byte[] buf, Position pos,
            TableMeta tableMeta) {
        List<BinlogColumnValue> columnValueList = new ArrayList<BinlogColumnValue>();
        byte[] rowNullBitmap = readRowColumnNullBitmap(buf, pos, colsNotNullBitmap);
        int nullBitmapIndex = 0;
        for (int i = 0; i < columnCount; i++) {
            ColumnDefinition columnDef = tableMeta.getColumnDefinition(i);

            // row base binlog hasn't this column
            if (isNullColumnInTable(i)) {
                columnValueList.add(new BinlogColumnValue(tableMeta.getColumnDefinition(i), null));
                continue;
            }
            // parse value as per column type
            // could be null in current row
            // ColumnDefinition columnDef = tableMeta
            // .getColumnDefinition(i);
            if (isNullInRow(nullBitmapIndex, rowNullBitmap)) {
                columnValueList.add(new BinlogColumnValue(columnDef, null));
            } else {
                int meta = tableMapEvent.getColumnDefList().get(i).getMeta();
                ColumnType typeInBinlog = tableMapEvent.getColumnDefList().get(i).getColumnType();
                parseEachColumnOfRow(buf, pos, columnValueList, columnDef, meta, typeInBinlog);
            }
            nullBitmapIndex++;

        }

        return columnValueList;
    }

    private void parseEachColumnOfRow(byte[] buf, Position pos, final List<BinlogColumnValue> columnValueList,
            ColumnDefinition columnDef, int meta, ColumnType typeInBinlog) {
        if (typeInBinlog != ColumnType.MYSQL_TYPE_STRING) {
            doParseColumn(buf, pos, columnValueList, columnDef, meta, typeInBinlog);
            return;
        }
        int[] lengthHolder = { 0 };
        ColumnType realType = null;
        try {
            realType = prepareForTypeString(meta, lengthHolder);
        } catch (InvalidColumnType e) {
            LOG.info("{},{}", tableMapEvent.getTableName(), columnDef.getColumName());
            throw e;
        }
        if (realType == ColumnType.MYSQL_TYPE_STRING) {
            doParseColumn(buf, pos, columnValueList, columnDef, lengthHolder[0], realType);
            return;
        }

        doParseColumn(buf, pos, columnValueList, columnDef, meta, realType);
    }

    
    private void doParseColumn(byte[] buf, Position pos, final List<BinlogColumnValue> columnValueList,
            ColumnDefinition columnDef, int meta, ColumnType columnType) {
        try {
            columnValueList.add(new BinlogColumnValue(columnDef,
                    ColumnTypeValueParserFactory.factory(columnType).parse(buf, pos, columnDef, meta)));
        } catch (RuntimeException e) {
            BinlogColumnValue firstValue = null;
            if (columnValueList.size() > 0) {
                firstValue = columnValueList.get(0);
            }
            LOG.error(
                    "parse column value error, maybe you add new column in middle of all columns.{},{},binlog type:{},table type {},charset {}, first column value {}",
                    tableMapEvent.getTableName(), columnDef.getColumName(), columnType.getTypeValue(),
                    columnDef.getType().getTypeValue(), columnDef.getCharset().getCharsetName(), firstValue);
            throw new FetalParseValueExp(e);
        }

    }

    private ColumnType prepareForTypeString(int meta, int[] lengthHolder) {
        if (meta >= 256) {

            int byte0 = meta & 0xff;
            int byte1 = meta >>> 8;

            if ((byte0 & 0x30) != 0x30) {
                /* a long CHAR() field: see #37426 */
                lengthHolder[0] = byte0 | (((byte1 & 0x30) ^ 0x30) << 4);
                return ColumnType.ofTypeValue(byte1 | 0x30);
            } else {
                lengthHolder[0] = meta & 0xff;
                return ColumnType.ofTypeValue(byte0);
            }
        } else {
            lengthHolder[0] = meta;
        }
        return ColumnType.MYSQL_TYPE_STRING;
    }

    private boolean isNullInRow(int nullBitmapIndex, byte[] rowNullBitmap) {
        int value = rowNullBitmap[nullBitmapIndex / 8];
        return (value & (1 << (nullBitmapIndex % 8))) != 0;
    }

    private boolean isNullColumnInTable(int index) {
        int flag = 0xff & columnsNotNullBitmap[index / 8];
        return (flag & (1 << (index % 8))) == 0;
    }

    private byte[] readRowColumnNullBitmap(byte[] buf, Position pos, byte[] bitmap) {
        return MysqlStringUtils.readFixString(buf, pos, calculateBytesLen(calculateColunmDataNullBitmapLen(bitmap)));
    }

    private int calculateBytesLen(int size) {
        return (size + 7) / 8;
    }

    private int calculateColunmDataNullBitmapLen(byte[] bitmap) {
        int len = 0;
        for (int i = 0; i < bitmap.length; i++) {
            len += Integer.bitCount(bitmap[i] & 0xff);
        }
        return len;
    }

    public List<BinlogResultRow> getRowList() {
        return rowList;
    }

    public TableMapEvent getTableMapEvent() {
        return tableMapEvent;
    }
}
