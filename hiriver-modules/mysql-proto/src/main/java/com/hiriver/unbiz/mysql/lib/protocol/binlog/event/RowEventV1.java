package com.hiriver.unbiz.mysql.lib.protocol.binlog.event;

import java.util.List;

import com.hiriver.unbiz.mysql.lib.output.BinlogColumnValue;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TableMeta;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TableMetaProvider;

/**
 * 行事件，版本2，支持获取update之前的数据。在子header后有2个字节的预留，解析时需要跳过。
 * mysql文档中在这个地方存在错误
 * 
 * @author hexiufeng
 *
 */
public class RowEventV1 extends BaseRowEvent implements BinlogEvent {
    private byte[] updateColumnsNotNullBitmap;

    public RowEventV1(TableMapEvent tableMapEvent, TableMetaProvider tableMetaProvider, int eventType,
            final long eventBinlogPos, boolean hasCheckSum) {
        super(tableMapEvent, tableMetaProvider, eventType, eventBinlogPos, hasCheckSum);
    }

    @Override
    protected void parseVerPostHeader(byte[] buf, Position pos) {
        // 跳过2个字节的预留
    	pos.forwardPos(2);
    }

    @Override
    protected void parseVerBodyForUpdate(byte[] buf, Position pos) {
        updateColumnsNotNullBitmap = readNotNullBitmap(buf, pos);
    }

    @Override
    protected List<BinlogColumnValue> parseVerRowForUpdate(byte[] buf, Position pos, TableMeta tableMeta) {
        return super.parseRow(updateColumnsNotNullBitmap, buf, pos, tableMeta);
    }
}
