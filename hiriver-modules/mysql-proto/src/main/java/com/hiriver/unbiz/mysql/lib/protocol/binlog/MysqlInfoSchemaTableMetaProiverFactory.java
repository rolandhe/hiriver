package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.ColumnType;
import com.hiriver.unbiz.mysql.lib.TextProtocolBlockingTransport;
import com.hiriver.unbiz.mysql.lib.TextProtocolBlockingTransportImpl;
import com.hiriver.unbiz.mysql.lib.TransportConfig;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.TableMapEvent;
import com.hiriver.unbiz.mysql.lib.protocol.text.ResultsetRowResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 从information_schema.columns 表读取元数据集信息，需要user开权限
 *
 * Beta
 */
public class MysqlInfoSchemaTableMetaProiverFactory implements TableMetaProiverFactory {
    @Override
    public TableMetaProvider factory(final String host, final int port,final String userName, final String password, final TransportConfig transportConfig) {
        final Map<String, TableMeta> cache = new HashMap<String, TableMeta>();
        return new TableMetaProvider() {
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
                TextProtocolBlockingTransport textTrans = new TextProtocolBlockingTransportImpl(host, port,
                        userName, password, "information_schema", transportConfig);

                textTrans.open();
                try {
                    readFieldListMeta(schemaName,tableName, tableMeta, textTrans);
                } finally {
                    textTrans.close();
                }
                cache.put(fullTableName, tableMeta);
                return tableMeta;
            }
            private void readFieldListMeta(String dbName,String tableName, TableMeta tableMeta, TextProtocolBlockingTransport textTrans) {
                TextCommandQueryResponse response =  textTrans.execute("select * from information_schema.columns where TABLE_SCHEMA='" + dbName + "' and TABLE_NAME='" + tableName + "'");
                List<ResultsetRowResponse>  list = response.getRowList();
                for(ResultsetRowResponse row : list) {
                    ColumnDefinition def = new ColumnDefinition();

                    def.setColumName(row.getValueList().get(3).getValueAsString());
                    def.setCharset(row.getValueList().get(13).getValueAsString());
                    def.setType(ColumnType.ofTypeName(row.getValueList().get(7).getValueAsString()));
                    if(!row.getValueList().get(8).isNull()) {
                        def.setLen(row.getValueList().get(8).getValueAsInt());
                    }
                    String key = row.getValueList().get(16).getValueAsString();
                    def.setPrimary("PRI".equals(key));

                    def.setUnique("UNI".equals(key));
                    def.setKey("MUL".equals(key));
                    if(!row.getValueList().get(15).isNull()) {
                        String ct = row.getValueList().get(15).getValueAsString();
                        def.setUnsigned(ct.indexOf("unsigned")>=0);
                    }
                    tableMeta.addColumn(def);
                }
            }
        };
    }
}
