package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.AbstractRequest;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.SafeByteArrayOutputStream;
import com.hiriver.unbiz.mysql.lib.protocol.tool.StringTool;

/**
 * 基于binlog file name + offset的dump binlog指令实现,适用于COM_BINLOG_DUMP指令
 * 
 * @author hexiufeng
 *
 */
public class DumpRequest extends AbstractRequest {
    private final long pos;
    private final int serverId;
    private final String binlogFileName;

    public DumpRequest(long pos, int serverId, String binlogFileName) {
        this.pos = pos;
        this.serverId = serverId;
        this.binlogFileName = binlogFileName;
    }

    @Override
    protected void fillPayload(SafeByteArrayOutputStream out) {
        out.write(0x12);
        out.safeWrite(MysqlNumberUtils.write4Int((int) pos));
        out.safeWrite(MysqlNumberUtils.writeNInt(2, 2));
        out.safeWrite(MysqlNumberUtils.write4Int(serverId));
        out.safeWrite(StringTool.safeConvertString2Bytes(binlogFileName));
    }

}
