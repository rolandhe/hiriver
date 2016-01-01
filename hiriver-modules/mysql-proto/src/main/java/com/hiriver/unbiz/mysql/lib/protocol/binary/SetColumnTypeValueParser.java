package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

/**
 * mysql set类型的解析器，它和枚举一样，本质上是string，一般不用
 * 
 * @author hexiufeng
 *
 */
public class SetColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        int len = (meta >> 8) * 8;
        return MysqlStringUtils.readFixString(buf, pos, len);
    }

}
