package com.hiriver.unbiz.mysql.lib.protocol.binlog.event;

import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

/**
 * 执行sql语句的事件，在binlog中一般保持开启、提交、回滚事件和ddl
 * 
 * @author hexiufeng
 *
 */
public class QueryEvent extends AbstractBinlogEvent implements BinlogEvent {
    private int slaveProxyId;
    private int executionTime;
    private int schemaLength;
    private int errorCode;
    private int statusVarsLength;
    private byte[] statusVars;
    private String schema;
    private String query;

    public QueryEvent(long eventBinlogPos, boolean hasCheckSum) {
        super(eventBinlogPos, hasCheckSum);
    }

    @Override
    public void parse(byte[] buf, Position pos) {
        this.slaveProxyId = MysqlNumberUtils.read4Int(buf, pos);
        this.executionTime = MysqlNumberUtils.read4Int(buf, pos);
        this.schemaLength = MysqlNumberUtils.read1Int(buf, pos);
        this.errorCode = MysqlNumberUtils.read2Int(buf, pos);
        this.statusVarsLength = MysqlNumberUtils.read2Int(buf, pos);
        this.statusVars = MysqlStringUtils.readFixString(buf, pos, statusVarsLength);
        this.schema = new String(MysqlStringUtils.readFixString(buf, pos, schemaLength));
        pos.forwardPos();
        this.query = new String(MysqlStringUtils.readEofString(buf, pos, super.isHasCheckSum()));
    }

    public int getSlaveProxyId() {
        return slaveProxyId;
    }

    public int getExecutionTime() {
        return executionTime;
    }

    public int getSchemaLength() {
        return schemaLength;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public int getStatusVarsLength() {
        return statusVarsLength;
    }

    public byte[] getStatusVars() {
        return statusVars;
    }

    public String getSchema() {
        return schema;
    }

    public String getQuery() {
        return query;
    }

}
