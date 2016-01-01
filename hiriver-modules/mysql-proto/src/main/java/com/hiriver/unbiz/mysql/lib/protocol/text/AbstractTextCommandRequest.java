package com.hiriver.unbiz.mysql.lib.protocol.text;

import com.hiriver.unbiz.mysql.lib.protocol.AbstractRequest;

/**
 * 抽象的文本协议的 request命令
 * 
 * @author hexiufeng
 *
 */
public abstract class AbstractTextCommandRequest extends AbstractRequest {
    protected final int command;

    protected AbstractTextCommandRequest(int command) {
        this.command = command;
    }
}
