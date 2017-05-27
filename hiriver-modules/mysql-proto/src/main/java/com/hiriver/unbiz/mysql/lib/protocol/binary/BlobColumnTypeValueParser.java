package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.CharsetMapping;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.StringTool;

/**
 * blob or text，text和 blob在底层存储是相同的，只是blob类型的字符集是binary，而text类型有utf-8或者其他字符集
 * 
 * @author hexiufeng
 * 
 */
public class BlobColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        int len = (int) MysqlNumberUtils.readNInt(buf, pos, meta);
        if (CharsetMapping.isBinary(columnDef.getCharset())) {
            return MysqlStringUtils.readFixString(buf, pos, len);
        } else {
            return StringTool.safeConvertBytes2String(MysqlStringUtils.readFixString(buf, pos, len),
                    columnDef.getCharset());
        }

    }
}
