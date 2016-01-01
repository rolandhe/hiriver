package com.hiriver.unbiz.mysql.lib.protocol.binlog.event;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogContext;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEventType;

/**
 * 根据事件类型产生事件对象的工厂
 * 
 * @author hexiufeng
 *
 */
public class EventFactory {
    public static BinlogEvent factory(final int eventType, final long binlogEventPos, final BinlogContext context,
            boolean hasCheckSum) {
        switch (eventType) {
            case BinlogEventType.FORMAT_DESCRIPTION_EVENT:
                return new FormatDescriptionEvent(binlogEventPos, hasCheckSum);
            case BinlogEventType.GTID_EVENT:
                return new GTidEvent(binlogEventPos, hasCheckSum);
            case BinlogEventType.QUERY_EVENT:
                return new QueryEvent(binlogEventPos, hasCheckSum);
            case BinlogEventType.ROTATE_EVENT:
                return new RotateEvent(binlogEventPos, hasCheckSum);
            case BinlogEventType.WRITE_ROWS_EVENTv0:
            case BinlogEventType.UPDATE_ROWS_EVENTv0:
            case BinlogEventType.DELETE_ROWS_EVENTv0:
                return new RowEventV0(context.getTableMapEvent(), context.getTableMetaProvider(), eventType,
                        binlogEventPos, hasCheckSum);
            case BinlogEventType.WRITE_ROWS_EVENTv1:
            case BinlogEventType.UPDATE_ROWS_EVENTv1:
            case BinlogEventType.DELETE_ROWS_EVENTv1:
                return new RowEventV1(context.getTableMapEvent(), context.getTableMetaProvider(), eventType,
                        binlogEventPos, hasCheckSum);
            case BinlogEventType.WRITE_ROWS_EVENTv2:
            case BinlogEventType.UPDATE_ROWS_EVENTv2:
            case BinlogEventType.DELETE_ROWS_EVENTv2:
                return new RowEventV2(context.getTableMapEvent(), context.getTableMetaProvider(), eventType,
                        binlogEventPos, hasCheckSum);
            case BinlogEventType.STOP_EVENT:
                return new StopEvent(binlogEventPos, hasCheckSum);
            case BinlogEventType.TABLE_MAP_EVENT:
                return new TableMapEvent(context.getForamtDescEvent(), binlogEventPos, hasCheckSum);
            case BinlogEventType.XID_EVENT:
                return new XidEvent(binlogEventPos, hasCheckSum);
            // case BinlogEventType.PREVIOUS_GTIDS_EVENT:
            // return new GTidEvent(binlogEventPos);
            default:
                return new UnkonwnEvent(binlogEventPos, hasCheckSum);
        }
    }
}
