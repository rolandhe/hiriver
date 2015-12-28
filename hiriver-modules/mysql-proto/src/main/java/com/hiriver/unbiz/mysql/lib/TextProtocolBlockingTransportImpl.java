package com.hiriver.unbiz.mysql.lib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiriver.unbiz.mysql.lib.protocol.OKPacket;
import com.hiriver.unbiz.mysql.lib.protocol.Response;
import com.hiriver.unbiz.mysql.lib.protocol.text.FieldListCommandResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.PingRequest;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandFieldListRequest;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryRequest;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryResponse;
import com.hiriver.unbiz.mysql.lib.protocol.tool.PacketTool;

/**
 * 支持Text Query协议的Transport实现
 * 
 * @author hexiufeng
 * 
 */
public class TextProtocolBlockingTransportImpl extends AbstractBlockingTransport
        implements TextProtocolBlockingTransport {
    private static final Logger LOGGER = LoggerFactory.getLogger(TextProtocolBlockingTransportImpl.class);

    /**
     * 在解析Response时需要持续读取数据的ResultContentReader接口默认实现
     */
    private final ResultContentReader resultContentReader = new ResultContentReader() {

        @Override
        public byte[] readNextPacketPayload() {
            return readResponsePayload();
        }

    };

    public TextProtocolBlockingTransportImpl() {
        super();
    }

    public TextProtocolBlockingTransportImpl(String host, int port, String userName, String password) {
        super(host, port, userName, password);
    }

    public TextProtocolBlockingTransportImpl(String host, int port, String userName, String password,
            TransportConfig transportConfig) {
        super(host, port, userName, password);
        super.setTransportConfig(transportConfig);
    }

    public TextProtocolBlockingTransportImpl(String host, int port, String userName, String password, String defDbName,
            TransportConfig transportConfig) {
        super(host, port, userName, password, defDbName);
        super.setTransportConfig(transportConfig);
    }

    public TextProtocolBlockingTransportImpl(String host, int port, String userName, String password,
            String defDbName) {
        super(host, port, userName, password, defDbName);
    }

    @Override
    public int executeSQL(String sql) {
        Response resp = executeSQLCore(sql);
        if (resp instanceof OKPacket) {
            return (int) ((OKPacket) resp).getAffectedRows();
        }
        return 0;
    }

    @Override
    public TextCommandQueryResponse execute(String sql) {
        Response resp = executeSQLCore(sql);
        if (resp instanceof TextCommandQueryResponse) {
            return (TextCommandQueryResponse) resp;
        }
        return null;
    }

    @Override
    public FieldListCommandResponse showFieldList(TextCommandFieldListRequest fieldListRequest) {
        checkFieldListRequest(fieldListRequest);
        super.writeRequest(fieldListRequest);
        byte[] buffer = super.readResponsePayload();
        FieldListCommandResponse response = new FieldListCommandResponse(resultContentReader);
        response.parse(buffer);
        return response;
    }

    @Override
    public boolean ping() {
        PingRequest pingReq = new PingRequest();
        try {
            super.writeRequest(pingReq);
            OKPacket okPacket = new OKPacket();
            okPacket.parse(super.readResponsePayload());
        } catch (RuntimeException e) {
            LOGGER.error("ping command", e);
            return false;
        }
        return true;
    }

    @Override
    protected Logger getSubClassLogger() {
        return LOGGER;
    }

    /**
     * 验证指定的TextCommandFieldListRequest是否有效
     * 
     * @param fieldListRequest TextCommandFieldListRequest
     */
    private void checkFieldListRequest(TextCommandFieldListRequest fieldListRequest) {
        if (null == fieldListRequest || fieldListRequest.getTable() == null || fieldListRequest.getTable().isEmpty()) {
            throw new IllegalArgumentException("invalid field list command parameters");
        }
    }

    @Override
    protected void intiTransport(String sql) {
        LOGGER.info("init SQL:" + sql);
        executeSQL(sql);
    }

    /**
     * 执行sql命令的内部实现方法
     * 
     * @param sql sql命令
     * @return 响应对象
     */
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
}
