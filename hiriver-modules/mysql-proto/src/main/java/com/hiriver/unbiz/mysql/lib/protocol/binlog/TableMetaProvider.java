package com.hiriver.unbiz.mysql.lib.protocol.binlog;

public interface TableMetaProvider {
    TableMeta getTableMeta(long tableId, String schemaName, String tableName);
}
