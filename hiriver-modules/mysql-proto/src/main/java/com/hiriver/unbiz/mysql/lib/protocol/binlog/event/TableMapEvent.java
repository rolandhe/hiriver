package com.hiriver.unbiz.mysql.lib.protocol.binlog.event;

import java.util.ArrayList;
import java.util.List;

import com.hiriver.unbiz.mysql.lib.ColumnType;
import com.hiriver.unbiz.mysql.lib.filter.TableFilter;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEventType;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.InternelColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.InvalidColumnType;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * tablemapevent，描述表的元数据
 * 
 * @author hexiufeng
 *
 */
public class TableMapEvent extends AbstractBinlogEvent implements BinlogEvent {
    private static  final Logger LOGGER = LoggerFactory.getLogger(TableMapEvent.class);

    private final FormatDescriptionEvent formatDescriptionEvent;

    private long tableId;
    private int flag;
    private int schemaLen;
    private String schema;
    private int tableNameLen;
    private String tableName;
    private int columnCount;
    private byte[] columnTypeDef;
    private byte[] columnMetaDef;
    private byte[] nullBitmap;
    private boolean containsEnumOrSet;

    private List<InternelColumnDefinition> columnDefList;

    private TableFilter tableFilter;

    public TableMapEvent(FormatDescriptionEvent formatDescriptionEvent, final long eventBinlogPos,
            boolean hasCheckSum) {
        super(eventBinlogPos, hasCheckSum);
        this.formatDescriptionEvent = formatDescriptionEvent;
    }

    @Override
    public void parse(byte[] buf, Position pos) {
        if (formatDescriptionEvent.getPostHeaderLen(BinlogEventType.FORMAT_DESCRIPTION_EVENT) == 6) {
            tableId = MysqlNumberUtils.read4Int(buf, pos);
        } else {
            tableId = MysqlNumberUtils.read6Int(buf, pos);
        }
        flag = MysqlNumberUtils.read2Int(buf, pos);

        this.schemaLen = MysqlNumberUtils.read1Int(buf, pos);
        this.schema = new String(MysqlStringUtils.readFixString(buf, pos, schemaLen));
        pos.forwardPos();
        this.tableNameLen = MysqlNumberUtils.read1Int(buf, pos);
        this.tableName = new String(MysqlStringUtils.readFixString(buf, pos, tableNameLen));
        pos.forwardPos();
        this.columnCount = (int) MysqlNumberUtils.readLencodeLong(buf, pos);

        this.columnTypeDef = MysqlStringUtils.readFixString(buf, pos, columnCount);

        int metaLen = (int) MysqlNumberUtils.readLencodeLong(buf, pos);
        this.columnMetaDef = MysqlStringUtils.readFixString(buf, pos, metaLen);
        this.nullBitmap = MysqlStringUtils.readFixString(buf, pos, (columnCount + 7) / 8);

        // 如果设置了过滤条件，并且过滤不通过，不再继续解析，快速退出
        if(tableFilter != null && !tableFilter.filter(schema,tableName)) {
            LOGGER.info("{}.{} is filtered, quick exit.", schema , tableName);
            return;
        }

        createColumnDefList();
        for(InternelColumnDefinition columnDefinition:columnDefList){
            if(columnDefinition.isEnumOrSet()){
                this.containsEnumOrSet = true;
                break;
            }
        }
    }

    private void createColumnDefList() {
        columnDefList = new ArrayList<InternelColumnDefinition>(columnCount);
        Position pos = Position.factory();
        for (int i = 0; i < columnCount; i++) {
            ColumnType type = ColumnType.ofTypeValue(columnTypeDef[i] & 0xff);
            int meta = 0;
            if (type.getMataLen() > 0) {
                if (type == ColumnType.MYSQL_TYPE_STRING) {
                    meta = (int) MysqlNumberUtils.readBigEdianNInt(columnMetaDef, pos, type.getMataLen());
                } else {
                    meta = (int) MysqlNumberUtils.readNInt(columnMetaDef, pos, type.getMataLen());
                }
            }

            boolean isNull = (nullBitmap[i / 8] & (1 << (i % 8))) != 0;
            columnDefList.add(new InternelColumnDefinition(type, meta, isNull));
        }
        if(pos.getPos() < columnMetaDef.length){
            throw new RuntimeException("meta info parse error.");
        }
    }

    public String getFullTableName() {
        if (this.schema != null && this.schema.length() > 0) {
            return this.schema + "." + this.tableName;
        }
        return this.tableName;
    }

    public long getTableId() {
        return tableId;
    }

    public int getFlag() {
        return flag;
    }

    public int getSchemaLen() {
        return schemaLen;
    }

    public String getSchema() {
        return schema;
    }

    public int getTableNameLen() {
        return tableNameLen;
    }

    public String getTableName() {
        return tableName;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public byte[] getColumnTypeDef() {
        return columnTypeDef;
    }

    public byte[] getColumnMetaDef() {
        return columnMetaDef;
    }

    public byte[] getNullBitmap() {
        return nullBitmap;
    }
    
    public List<InternelColumnDefinition> getColumnDefList() {
        return columnDefList;
    }

    public boolean hasEnumOrSet(){
        return containsEnumOrSet;
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }
}
