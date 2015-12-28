package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.FormatDescriptionEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.RotateEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.TableMapEvent;

public class BinlogContext {
    private TableMetaProvider tableMetaProvider;
    private FormatDescriptionEvent foramtDescEvent;
    private TableMapEvent tableMapEvent;
    private RotateEvent rotateEvent;

    public TableMetaProvider getTableMetaProvider() {
        return tableMetaProvider;
    }

    public FormatDescriptionEvent getForamtDescEvent() {
        return foramtDescEvent;
    }

    public TableMapEvent getTableMapEvent() {
        return tableMapEvent;
    }

    public void setTableMetaProvider(TableMetaProvider tableMetaProvider) {
        this.tableMetaProvider = tableMetaProvider;
    }

    public void setForamtDescEvent(FormatDescriptionEvent foramtDescEvent) {
        this.foramtDescEvent = foramtDescEvent;
    }

    public void setTableMapEvent(TableMapEvent tableMapEvent) {
        this.tableMapEvent = tableMapEvent;
    }

    public RotateEvent getRotateEvent() {
        return rotateEvent;
    }

    public void setRotateEvent(RotateEvent rotateEvent) {
        this.rotateEvent = rotateEvent;
    }
}
