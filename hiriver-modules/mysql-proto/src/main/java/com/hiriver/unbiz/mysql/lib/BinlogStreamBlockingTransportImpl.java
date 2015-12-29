package com.hiriver.unbiz.mysql.lib;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiriver.unbiz.mysql.lib.filter.TableFilter;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.ERRPacket;
import com.hiriver.unbiz.mysql.lib.protocol.OKPacket;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.Request;
import com.hiriver.unbiz.mysql.lib.protocol.Response;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogContext;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEventHeader;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.RegisterRequest;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TableMeta;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TableMetaProvider;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidEventType;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.BaseRowEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.EventFactory;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.FormatDescriptionEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.GTidEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.QueryEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.RotateEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.TableMapEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.XidEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.ReadTimeoutExp;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.text.ColumnDefinitionResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.FieldListCommandResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandFieldListRequest;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryRequest;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryResponse;
import com.hiriver.unbiz.mysql.lib.protocol.tool.PacketTool;
import com.hiriverunbiz.mysql.lib.exp.InvalidMysqlDataException;

public class BinlogStreamBlockingTransportImpl extends AbstractBlockingTransport
        implements BinlogStreamBlockingTransport {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogStreamBlockingTransportImpl.class);
    private final BinlogContext context = new BinlogContext();
    private int serverId;
    private TableFilter tableFilter;
    private boolean checkSum = true;
    
    private Position defaultPos = Position.factory();
    /**
     * 在解析Response时需要持续读取数据的ResultContentReader接口默认实现
     */
    private final ResultContentReader resultContentReader = new ResultContentReader() {

        @Override
        public byte[] readNextPacketPayload() {
            return readResponsePayload();
        }

    };
    private TableMetaProvider tableMetaProvider = new TableMetaProvider() {
        private final Map<String, TableMeta> cache = new HashMap<String, TableMeta>();

        @Override
        public TableMeta getTableMeta(long tableId, String schemaName, String tableName) {
            String fullTableName = schemaName + "." + tableName;
            if (cache.containsKey(fullTableName)) {
                TableMeta tableMeta = cache.get(fullTableName);
                if (tableMeta.getTableId() == tableId) {
                    return tableMeta;
                }
            }
            TableMeta tableMeta = new TableMeta(tableId);
            TextProtocolBlockingTransport textTrans = new TextProtocolBlockingTransportImpl(getHost(), getPort(),
                    getUserName(), getPassword(), schemaName, getTransportConfig());

            textTrans.open();
            try {
                TextCommandFieldListRequest request = new TextCommandFieldListRequest(tableName);
                FieldListCommandResponse response = textTrans.showFieldList(request);
                for (ColumnDefinitionResponse coldef : response.getColumnList()) {
                    ColumnDefinition def = createColumnDefinition(coldef);
                    tableMeta.addColumn(def);
                }

            } finally {
                textTrans.close();
            }
            cache.put(fullTableName, tableMeta);
            return tableMeta;
        }

        private ColumnDefinition createColumnDefinition(ColumnDefinitionResponse coldef) {
            ColumnDefinition def = new ColumnDefinition();
            def.setColumName(coldef.getName());
            def.setCharset(coldef.getCharset());
            def.setKey(coldef.isKey());
            def.setPrimary(coldef.isPrimayKey());
            def.setType(coldef.getType());
            def.setUnique(coldef.isUniqueKey());
            def.setUnsigned(coldef.isUnsigned());
            return def;
        }

    };

    public BinlogStreamBlockingTransportImpl() {

    }

    public BinlogStreamBlockingTransportImpl(String host, int port, String userName, String password) {
        super(host, port, userName, password, null);
    }

    @Override
    public boolean ping() {
        return false;
    }

    @Override
    protected Logger getSubClassLogger() {
        return LOGGER;
    }

    public boolean isCheckSum() {
        return checkSum;
    }

    public void setCheckSum(boolean checkSum) {
        this.checkSum = checkSum;
    }

    @Override
    protected void intiTransport(String sql) {
        if (checkSum) {
            int affectRows = executeSQL("SET @master_binlog_checksum= @@global.binlog_checksum");
            getSubClassLogger().info("set checkSum, result is {}.", affectRows);
        }
    }

    private int executeSQL(String sql) {
        Response resp = executeSQLCore(sql);
        if (resp instanceof OKPacket) {
            return (int) ((OKPacket) resp).getAffectedRows();
        }
        return 0;
    }

    private Response executeSQLCore(String sql) {
        TextCommandQueryRequest query = new TextCommandQueryRequest(sql);
        super.writeRequest(query);
        byte[] buffer = super.readResponsePayload();
        checkErrPacket(buffer);
        if (PacketTool.isOkPackete(buffer)) {
            OKPacket okPacket = new OKPacket();
            okPacket.parse(buffer);
            return okPacket;
        }
        TextCommandQueryResponse queryResp = new TextCommandQueryResponse(resultContentReader);
        queryResp.parse(buffer);
        return queryResp;
    }

    @Override
    public void dump(BinlogPosition binlogPos) {
        context.setTableMetaProvider(tableMetaProvider);
        super.open();
        registerSlave();
        
        super.writeRequest(binlogPos.packetDumpRequest(this.serverId));
        readFormatEvent();
        super.readTimeoutHanlder = new SocketReadTimeoutHanlder() {

            @Override
            public void handle(String message, Exception e) {
                LOGGER.error(message, e);
                throw new ReadTimeoutExp();
            }

        };
    }

    private void registerSlave() {
        // register
        Request reg = new RegisterRequest(serverId);
        super.writeRequest(reg);
        byte[] buf = super.readResponsePayload();

        if (!PacketTool.isOkPackete(buf)) {
            ERRPacket ep = new ERRPacket();
            ep.setCheckSum(this.checkSum);
            ep.parse(buf);
            throw new InvalidMysqlDataException(ep.getErrorMessage());
        }
    }

    @Override
    public ValidBinlogOutput getBinlogOutput() {
        Position pos = Position.factory();
        while (true) {
            BinlogEvent event = readEvent(pos);
            if (event instanceof TableMapEvent) {
                context.setTableMapEvent((TableMapEvent) event);
            }
            ValidBinlogOutput ve = distinguishEvent(event);
            if (ve != null) {
                return ve;
            }
        }
    }

    @Override
    public ValidBinlogOutput getBinlogOutputImmediately() {
        
        BinlogEvent event = readEvent(defaultPos);
        if (event instanceof TableMapEvent) {
            context.setTableMapEvent((TableMapEvent) event);
            return null;
        }
        return distinguishEvent(event);
    }

    private ValidBinlogOutput distinguishEvent(BinlogEvent event) {
        if (event instanceof GTidEvent) {
            return new ValidBinlogOutput(event, context.getRotateEvent().getNextBinlogName(), ValidEventType.GTID);
        }
        if (event instanceof BaseRowEvent) {
            return new ValidBinlogOutput(event, context.getRotateEvent().getNextBinlogName(), ValidEventType.ROW);
        }
        if (event instanceof XidEvent) {
            return new ValidBinlogOutput(event, context.getRotateEvent().getNextBinlogName(),
                    ValidEventType.TRANS_COMMIT);
        }
        if (event instanceof QueryEvent) {
            QueryEvent qEvent = (QueryEvent) event;
            if ("BEGIN".equals(qEvent.getQuery())) {
                return new ValidBinlogOutput(event, context.getRotateEvent().getNextBinlogName(),
                        ValidEventType.TRAN_BEGIN);
            }
            if ("ROLLBACK".equals(qEvent.getQuery())) {
                return new ValidBinlogOutput(event, context.getRotateEvent().getNextBinlogName(),
                        ValidEventType.TRANS_ROLLBACK);
            }
            return null;
        }

        return null;
    }

    private void readFormatEvent() {
        Position pos = Position.factory();
        while (true) {
            BinlogEvent event = readEvent(pos);
            if (event instanceof RotateEvent) {
                context.setRotateEvent((RotateEvent) event);
                continue;
            }
            if (event instanceof FormatDescriptionEvent) {
                context.setForamtDescEvent((FormatDescriptionEvent) event);
                break;
            }
        }
    }

    private BinlogEvent readEvent(Position pos) {
        byte[] buf = super.readResponsePayload();

        if (!PacketTool.isOkPackete(buf)) {
            ERRPacket ep = new ERRPacket();
            ep.setCheckSum(this.checkSum);
            ep.parse(buf);
            throw new InvalidMysqlDataException(ep.getErrorMessage());
        }

        pos.reset();
        pos.forwardPos();
        BinlogEventHeader eventHeader = new BinlogEventHeader();

        eventHeader.parse(buf, pos);
        BinlogEvent event =
                EventFactory.factory(eventHeader.getEventType(), eventHeader.getLogPos(), context, checkSum);
        if (!filter(event)) {
            return null;
        }
        event.parse(buf, pos);
        event.acceptOccurTime(eventHeader.getTimestamp());
        return event;
    }

    private boolean filter(BinlogEvent event) {
        if (this.tableFilter == null) {
            return true;
        }
        if (event instanceof BaseRowEvent) {
            BaseRowEvent rowEvent = (BaseRowEvent) event;
            
            
            boolean ret =  this.tableFilter.filter(rowEvent.getTableMapEvent().getSchema(),
                    rowEvent.getTableMapEvent().getTableName());
            
            LOGGER.debug("filter row event,{}.{}, {} ",rowEvent.getTableMapEvent().getSchema(),rowEvent.getTableMapEvent().getTableName(),ret);
            if(ret){
            	return true;
            }
            return ret;
        } else {
            return true;
        }
    }

    public TableMetaProvider getTableMetaProvider() {
        return tableMetaProvider;
    }

    public void setTableMetaProvider(TableMetaProvider tableMetaProvider) {
        this.tableMetaProvider = tableMetaProvider;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public TableFilter getTableFilter() {
        return tableFilter;
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }
}
