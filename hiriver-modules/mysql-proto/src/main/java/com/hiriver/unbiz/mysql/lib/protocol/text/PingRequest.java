package com.hiriver.unbiz.mysql.lib.protocol.text;

import com.hiriver.unbiz.mysql.lib.protocol.Request;
import com.hiriver.unbiz.mysql.lib.protocol.tool.SafeByteArrayOutputStream;

/**
 * Ping command request
 * 
 * @author hexiufeng
 *
 */
public class PingRequest extends AbstractTextCommandRequest implements Request {

    public PingRequest() {
        super(0x0e);
    }

    @Override
    protected void fillPayload(SafeByteArrayOutputStream out) {
        out.write(super.command);
    }

}
