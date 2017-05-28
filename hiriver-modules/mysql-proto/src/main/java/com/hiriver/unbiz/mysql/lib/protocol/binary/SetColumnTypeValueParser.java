package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * mysql set类型的解析器，它和枚举一样，本质上是string，一般不用
 * 
 * @author hexiufeng
 *
 */
public class SetColumnTypeValueParser implements ColumnTypeValueParser {

    private  static  final int[] ONES = {
            1,
            1 << 1,
            1 << 2,
            1 << 3,
            1 << 4,
            1 << 5,
            1 << 6,
            1 << 7

    };

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {

        byte[] v =MysqlStringUtils.readFixString(buf, pos, meta);
        StringBuilder sb = new StringBuilder();
        int index = 0;
        for(byte b : v) {
            for(int i = 0;i < 8; i++){
                if((b & ONES[i]) == ONES[i]) {
                    sb.append(columnDef.getSetList().get(index * 8 + i));
                    sb.append(",");
                }
            }
        }

        return sb.substring(0,sb.length() - 1);
    }

}
