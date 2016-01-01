package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;

/**
 * binlog数据解析器，根据不同的列数据类型不同，而有不同解析方式
 * 
 * @author hexiufeng
 *
 */
public interface ColumnTypeValueParser {
    /**
     * 根据binlog日志数据解析出对应的java类型数据。
     * 
     * @param buf 二进制的binlog数据
     * @param pos 用于记录buffer中已解析数据的位置
     * @param columnDef 列的类型
     * @param meta binlog中的该类型的meta信息
     * @return java数据类型
     */
    Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta);
}
