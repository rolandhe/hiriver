package com.hiriver.unbiz.mysql.lib.protocol.text;

import com.hiriver.unbiz.mysql.lib.protocol.AbstractRequest;

public abstract class AbstractTextCommandRequest extends AbstractRequest {
    protected final int command;

    protected AbstractTextCommandRequest(int command) {
        this.command = command;
    }
}
