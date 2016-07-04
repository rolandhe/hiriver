package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import java.util.Map;

import com.hiriver.unbiz.mysql.lib.protocol.AbstractRequest;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.GTSidTool;
import com.hiriver.unbiz.mysql.lib.protocol.tool.SafeByteArrayOutputStream;

/**
 * 基于gtid的dump指令实现，适用于COM_BINLOG_DUMP_GTID指令
 * 
 * @author hexiufeng
 *
 */
public class GTidDumpRequest extends AbstractRequest {
    private final int serverId;
    private final Map<String, GtId> gtidInfoMap;

    public GTidDumpRequest(Map<String, GtId> gtidInfoMap, int serverId) {
        this.gtidInfoMap = gtidInfoMap;
        this.serverId = serverId;
    }

    @Override
    protected void fillPayload(SafeByteArrayOutputStream out) {
        out.write(0x1e); // command
        out.safeWrite(MysqlNumberUtils.writeNInt(0x04, 2)); // flag
        out.safeWrite(MysqlNumberUtils.writeNInt(serverId, 4)); // server id
        out.safeWrite(MysqlNumberUtils.writeNInt(4, 4)); // binlog name size
        out.safeWrite(MysqlNumberUtils.writeNInt(0, 4)); // binlog name
        out.safeWrite(MysqlNumberUtils.writeNLong(4L, 8)); // binlog_pos

        out.safeWrite(MysqlNumberUtils.writeNInt(calDataLen(gtidInfoMap.size()), 4)); // datalen

        out.safeWrite(MysqlNumberUtils.writeNLong(gtidInfoMap.size(), 8)); // n_sids

        for (String uuid : gtidInfoMap.keySet()) {
            out.safeWrite(GTSidTool.convertSidString2DumpFormatBytes(uuid)); // sid
            GtId gi = gtidInfoMap.get(uuid);

            out.safeWrite(MysqlNumberUtils.writeNLong(1L, 8)); // n_intervals
            out.safeWrite(MysqlNumberUtils.writeNLong(gi.getInternel().getStart(), 8)); // start
            out.safeWrite(MysqlNumberUtils.writeNLong(gi.getInternel().getStop(), 8)); // stop
        }
    }

    private int calDataLen(int nSids) {
        return nSids * 40 + 8;
    }

}
