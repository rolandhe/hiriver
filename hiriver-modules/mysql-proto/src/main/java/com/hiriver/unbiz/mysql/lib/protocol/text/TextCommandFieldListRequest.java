package com.hiriver.unbiz.mysql.lib.protocol.text;

import com.hiriver.unbiz.mysql.lib.protocol.tool.SafeByteArrayOutputStream;
import com.hiriver.unbiz.mysql.lib.protocol.tool.StringTool;

/**
 * COM_FIELD_LIST指令实现，用于获取表的列定义
 * 
 * @author hexiufeng
 *
 */
public class TextCommandFieldListRequest extends AbstractTextCommandRequest {
    private String table; // string.nul
    private String fieldWildcard; // string.eof

    public TextCommandFieldListRequest() {
        super(0x04);
    }

    public TextCommandFieldListRequest(String table) {
        super(0x04);
        this.table = table;
    }

    @Override
    protected void fillPayload(SafeByteArrayOutputStream out) {
        out.write(super.command);
        out.safeWrite(StringTool.safeConvertString2Bytes(table));
        out.write(0);
        if (fieldWildcard != null && !fieldWildcard.isEmpty()) {
            out.safeWrite(StringTool.safeConvertString2Bytes(fieldWildcard));
        }
    }

    public String getTable() {
        return table;
    }

    public String getFieldWildcard() {
        return fieldWildcard;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setFieldWildcard(String fieldWildcard) {
        this.fieldWildcard = fieldWildcard;
    }

}
