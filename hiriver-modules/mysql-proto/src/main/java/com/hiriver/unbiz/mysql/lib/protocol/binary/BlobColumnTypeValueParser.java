package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.MyCharset;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.StringTool;

/**
 * blob or text
 * 
 * @author hexiufeng
 * 
 */
public class BlobColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        int len = (int) MysqlNumberUtils.readNInt(buf, pos, meta);
        if (columnDef.getCharset() == MyCharset.BINARY) {
            return MysqlStringUtils.readFixString(buf, pos, len);
        } else {
            return StringTool.safeConvertBytes2String(MysqlStringUtils.readFixString(buf, pos, len),
                    columnDef.getCharset().getCharsetName());
        }

    }
}
