package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.AbstractRequest;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.SafeByteArrayOutputStream;

/**
 * 注册从库到主库请求指令，实现COM_REGISTER_SLAVE
 * 
 * @author hexiufeng
 *
 */
public class RegisterRequest extends AbstractRequest {
    private final int serverId;

    public RegisterRequest(int serverId) {
        this.serverId = serverId;
    }

    @Override
    protected void fillPayload(SafeByteArrayOutputStream out) {
        out.write(0x15);
        out.safeWrite(MysqlNumberUtils.write4Int(serverId));
        out.write(0);
        out.write(0);
        out.write(0);
        out.safeWrite(MysqlNumberUtils.writeNInt(0, 2));
        out.safeWrite(MysqlNumberUtils.write4Int(0));
        out.safeWrite(MysqlNumberUtils.write4Int(0));
    }

}
