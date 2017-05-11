package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.MysqlDecimal;

import java.math.BigDecimal;

/**
 * decimal 数据类型解析器
 *
 *
 * @author hexiufeng
 *
 */
public class DecimalColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        int scale = (meta >>> 8);
        int precision = meta & 0xff;
        MysqlDecimal myDecimal = new MysqlDecimal(precision,scale);
        byte[] decBuf = MysqlStringUtils.readFixString(buf, pos, myDecimal.getBinSize());
        myDecimal.parse(decBuf);
        return myDecimal.toDecimal();
    }
}
