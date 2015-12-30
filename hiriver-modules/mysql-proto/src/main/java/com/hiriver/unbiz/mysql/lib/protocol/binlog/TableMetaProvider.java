package com.hiriver.unbiz.mysql.lib.protocol.binlog;

/**
 * 表的元数据提供者，缺省实现是从mysql server读取。
 * 
 * @author hexiufeng
 *
 */
public interface TableMetaProvider {
    /**
     * 根据表的id和表名称获取表元数据
     * 
     * @param tableId tableid
     * @param schemaName 表所在的db
     * @param tableName 表名称
     * @return 表的元数据
     */
    TableMeta getTableMeta(long tableId, String schemaName, String tableName);
}
