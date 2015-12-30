package com.hiriver.unbiz.mysql.lib.filter;

/**
 * 根据db、table名称进行过滤的抽象描述
 * 
 * @author hexiufeng
 *
 */
public interface TableFilter {
    /**
     * 过滤
     * 
     * @param dbName db名称
     * @param tableName 表名称
     * @return 是否通过过滤
     */
    boolean filter(String dbName,String tableName);
}
