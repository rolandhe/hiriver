package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.StringTool;

/**
 * var char, var string, string
 * 
 * @author hexiufeng
 * 
 */
public class StringColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        // 注意,当column type is Type_String时， 这儿的meta不是mysql的meta，已经被转换过
        // see BaseRowEvent#parseEachColumnOfRow
        // meta is the max length of the column
        int len = 0;
        if (meta < 256) {
            len = (int) MysqlNumberUtils.read1Int(buf, pos);
        } else {
            len = (int) MysqlNumberUtils.read2Int(buf, pos);
        }

        return StringTool.safeConvertBytes2String(MysqlStringUtils.readFixString(buf, pos, len),
                columnDef.getCharset().getCharsetName());

    }

}
