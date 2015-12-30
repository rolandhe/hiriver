package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.protocol.AbstractRequest;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.SafeByteArrayOutputStream;

/**
 * 基于gtid的dump指令实现，适用于COM_BINLOG_DUMP_GTID指令
 * 
 * @author hexiufeng
 *
 */
public class GTidDumpRequest extends AbstractRequest {
    private final int serverId;
    private final GTIDSet gtidset;

    public GTidDumpRequest(GTIDSet gtidset, int serverId) {
        this.gtidset = gtidset;
        this.serverId = serverId;
    }

    @Override
    protected void fillPayload(SafeByteArrayOutputStream out) {
        out.write(0x1e); // command
        out.safeWrite(MysqlNumberUtils.writeNInt(0x04, 2)); // flag
        out.safeWrite(MysqlNumberUtils.writeNInt(serverId, 4)); // server id
        out.safeWrite(MysqlNumberUtils.writeNInt(3, 4)); // binlog name size
        out.safeWrite(MysqlNumberUtils.writeNInt(0, 3)); // binlog name
        out.safeWrite(MysqlNumberUtils.writeNLong(4L, 8)); // binlog_pos
        GTIDInfo[] gtidInfoArray = gtidset.getAllGTIDSet();

        out.safeWrite(MysqlNumberUtils.writeNInt(calDataLen(gtidInfoArray.length), 4)); // datalen

        out.safeWrite(MysqlNumberUtils.writeNLong(gtidInfoArray.length, 8)); // n_sids

        for (GTIDInfo info : gtidInfoArray) {
            out.safeWrite(info.getUuidBytes()); // sid
            out.safeWrite(MysqlNumberUtils.writeNLong(1L, 8)); // n_intervals
            out.safeWrite(MysqlNumberUtils.writeNLong(1L, 8)); // start
            out.safeWrite(MysqlNumberUtils.writeNLong(info.getStop() == 1L ? 2L : info.getStop(), 8)); // stop
        }
    }

    private int calDataLen(int nids) {
        return 8 + 40 * nids;
    }

}
