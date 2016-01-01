package com.hiriver.unbiz.mysql.lib.protocol.text;

import com.hiriver.unbiz.mysql.lib.protocol.tool.SafeByteArrayOutputStream;
import com.hiriver.unbiz.mysql.lib.protocol.tool.StringTool;

/**
 * COM_QUERY指令实现，用于执行sql
 * 
 * @author hexiufeng
 *
 */
public class TextCommandQueryRequest extends AbstractTextCommandRequest {
    private String query;

    public TextCommandQueryRequest() {
        super(0x03);
    }

    public TextCommandQueryRequest(String query) {
        super(0x03);
        this.query = query;
    }

    @Override
    protected void fillPayload(SafeByteArrayOutputStream out) {
        out.write(super.command);
        out.safeWrite(StringTool.safeConvertString2Bytes(query));
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

}
