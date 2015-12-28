package com.hiriver.unbiz.mysql.lib.filter;

public interface TableFilter {
    boolean filter(String dbName,String tableName);
}
