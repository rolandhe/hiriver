package com.hiriver.unbiz.mysql.lib.protocol.binlog.event;

import java.util.Collections;
import java.util.List;

import com.hiriver.unbiz.mysql.lib.output.BinlogColumnValue;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TableMeta;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TableMetaProvider;

/**
 * 行事件，版本0
 * 
 * @author hexiufeng
 *
 */
public class RowEventV0 extends BaseRowEvent implements BinlogEvent {

    public RowEventV0(TableMapEvent tableMapEvent, TableMetaProvider tableMetaProvider, int eventType,
            final long eventBinlogPos, boolean hasCheckSum) {
        super(tableMapEvent, tableMetaProvider, eventType, eventBinlogPos, hasCheckSum);
    }

    @Override
    protected void parseVerPostHeader(byte[] buf, Position pos) {
    }

    @Override
    protected void parseVerBodyForUpdate(byte[] buf, Position pos) {
        // do nothing
    }

    @Override
    protected List<BinlogColumnValue> parseVerRowForUpdate(byte[] buf, Position pos, TableMeta tableMeta) {
        return Collections.emptyList();
    }
}
