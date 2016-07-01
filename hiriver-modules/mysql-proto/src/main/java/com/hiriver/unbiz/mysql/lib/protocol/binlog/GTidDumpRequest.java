package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import java.util.List;
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
    private final  Map<String, List<GTIDInfo>> gtidInfoMap;

    public GTidDumpRequest( Map<String, List<GTIDInfo>> gtidInfoMap, int serverId) {
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

        out.safeWrite(MysqlNumberUtils.writeNInt(calDataLen(gtidInfoMap), 4)); // datalen

        out.safeWrite(MysqlNumberUtils.writeNLong(gtidInfoMap.size(), 8)); // n_sids

        for (String uuid : gtidInfoMap.keySet()) {
            out.safeWrite(GTSidTool.convertSidString2DumpFormatBytes(uuid)); // sid
            List<GTIDInfo> internals = gtidInfoMap.get(uuid);

            out.safeWrite(MysqlNumberUtils.writeNLong(internals.size(), 8)); // n_intervals
            for (GTIDInfo gi : internals) {
                out.safeWrite(MysqlNumberUtils.writeNLong(gi.getStart(), 8)); // start
                out.safeWrite(MysqlNumberUtils.writeNLong(gi.getStop() == 1L?2L:gi.getStop(), 8)); // stop
            }
        }
    }

    private int calDataLen( Map<String, List<GTIDInfo>> gtidInfoMap) {
        int len = 8;
        for (String uuid : gtidInfoMap.keySet()) {
            len += 24; // uuid size + n_intervals
            
            List<GTIDInfo> internals = gtidInfoMap.get(uuid);
            len += internals.size() * 16;
        }
        
        return len;
    }

}
