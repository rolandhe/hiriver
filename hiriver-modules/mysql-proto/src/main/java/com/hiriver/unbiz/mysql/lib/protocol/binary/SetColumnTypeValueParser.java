package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

/**
 * mysql set类型的解析器，它和枚举一样，本质上是string，一般不用<br>
 * <p>
 *   mysql的set与枚举类似，也是在表结构的元数据中定义一组文本值，在数据存储内部值存文本的索引，
 *   从而降低存储空间，但与枚举不同的是一个set值可能存储多个值，比如 set定义为（a,b,c,d),但
 *   某条记录只存储(a,d),在mysql是使用位图索引实现的，set中每个元素索引使用位的位置代替，比如
 *   set(a,b,c,d)对应1111，那么(a,d)就是1001
 *
 * </p>
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
