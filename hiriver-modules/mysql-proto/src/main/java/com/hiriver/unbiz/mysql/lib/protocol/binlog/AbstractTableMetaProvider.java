package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.TextProtocolBlockingTransport;
import com.hiriver.unbiz.mysql.lib.TextProtocolBlockingTransportImpl;
import com.hiriver.unbiz.mysql.lib.TransportConfig;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.TableMapEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

abstract class AbstractTableMetaProvider implements TableMetaProvider {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractTableMetaProvider.class);
    final Map<String, TableMeta> cache = new HashMap<String, TableMeta>();
    @Override
    public TableMeta getTableMeta(long tableId, TableMapEvent tableMapEvent) {
        String schemaName = tableMapEvent.getSchema();
        String tableName = tableMapEvent.getTableName();
        String fullTableName = schemaName + "." + tableName;
        if (cache.containsKey(fullTableName)) {
            TableMeta tableMeta = cache.get(fullTableName);
            if (tableMeta.getTableId() == tableId) {
                return tableMeta;
            }
        }
        TableMeta tableMeta = new TableMeta(tableId);
        TextProtocolBlockingTransport textTrans = new TextProtocolBlockingTransportImpl(getHost(), getPort(),
                getUserName(), getPassword(), schemaName, getTransportConfig());

        textTrans.open();
        try {
            List<ColumnDefinition> list = readMeta(tableName,tableMapEvent,textTrans);
            if(list != null && list.size() > 0) {
                tableMeta.getColumnMetaList().addAll(list);
            }
        } finally {
            textTrans.close();
        }
        cache.put(fullTableName, tableMeta);
        return tableMeta;
    }

    protected abstract String getHost();
    protected abstract int getPort();
    protected abstract String getUserName();
    protected abstract String getPassword();
    protected abstract TransportConfig getTransportConfig();

    protected abstract List<ColumnDefinition> readMeta(String tableName,TableMapEvent tableMapEvent, TextProtocolBlockingTransport textTrans);
}
