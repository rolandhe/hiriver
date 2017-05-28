package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

/**
 * mysql枚举数据类型解析器，mysql枚举在mysql binlog内部本质上是string类型。代码逻辑来自mysql源码
 * 
 * @author hexiufeng
 *
 */
public class EnumColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        int value = 0;
        int flag = meta & 0xff;
        switch (flag) {
            case 1:
                value = MysqlNumberUtils.read1Int(buf, pos);
                break;
            case 2:
                value = (int) MysqlNumberUtils.readBigEdianNInt(buf, pos, 2);
                break;
            default:
        }
        return columnDef.getEnumList().get(value - 1);
    }

}
