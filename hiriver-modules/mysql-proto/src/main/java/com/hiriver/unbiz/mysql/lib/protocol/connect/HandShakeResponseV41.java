package com.hiriver.unbiz.mysql.lib.protocol.connect;

import java.util.LinkedHashMap;
import java.util.Map;

import com.hiriver.unbiz.mysql.lib.protocol.AbstractRequest;
import com.hiriver.unbiz.mysql.lib.protocol.CapabilityFlagConst;
import com.hiriver.unbiz.mysql.lib.protocol.Request;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.PassSecure;
import com.hiriver.unbiz.mysql.lib.protocol.tool.SafeByteArrayOutputStream;
import com.hiriver.unbiz.mysql.lib.protocol.tool.StringTool;

/**
 * 握手协议请求<br>
 * 
 * <a href= "http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse" >
 * http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet- Protocol::HandshakeResponse </a>
 * 
 * @author hexiufeng
 * 
 */
public class HandShakeResponseV41 extends AbstractRequest implements Request {
    private static final int DEF_PACK_SIZE = (1 << 24);
    private final HandShakeV10 handshake;

    private int maxSendPacketSize = DEF_PACK_SIZE; // 16M
    private int charSet = 33; // utf8_general_ci
    private byte[] reserved = new byte[23];
    private String userName;
    private String password;
    private String dbName;
    private Map<String, String> propMap = new LinkedHashMap<String, String>();

    private int capability;

    {
        propMap.put("_runtime_version", System.getProperty("java.version"));
        propMap.put("_client_version", "0.9.0");
        propMap.put("_client_name", "hiriver client");
        propMap.put("_client_license", "com.hiriver");
        propMap.put("_runtime_vendor", System.getProperty("java.vendor"));
    }

    public HandShakeResponseV41(HandShakeV10 handshake, int sequenceId, int maxSendPacketSize) {
        this.handshake = handshake;
        if (maxSendPacketSize > DEF_PACK_SIZE) {
            this.maxSendPacketSize = maxSendPacketSize;
        }
        super.setSequenceId(sequenceId);
    }

    @Override
    protected void fillPayload(SafeByteArrayOutputStream out) {
        if (withDbName()) {
            capability |= CapabilityFlagConst.CLIENT_CONNECT_WITH_DB;
        }
        capability |= CapabilityFlagConst.CLIENT_FOUND_ROWS;
        capability |= CapabilityFlagConst.CLIENT_LOCAL_FILES;
        capability |= CapabilityFlagConst.CLIENT_LONG_PASSWORD | CapabilityFlagConst.CLIENT_LONG_FLAG
                | CapabilityFlagConst.CLIENT_PROTOCOL_41 | CapabilityFlagConst.CLIENT_TRANSACTIONS
                | CapabilityFlagConst.CLIENT_SECURE_CONNECTION | CapabilityFlagConst.CLIENT_CONNECT_ATTRS;
        out.safeWrite(MysqlNumberUtils.write4Int(capability));
        out.safeWrite(MysqlNumberUtils.write4Int(maxSendPacketSize));
        out.write(charSet);
        out.safeWrite(reserved);
        out.safeWrite(StringTool.safeConvertString2Bytes(userName));
        out.write(0);
        if (this.password == null || this.password.length() == 0) {
            out.write(0x00);
        } else {
            // mysql server可能支持native password plugin方式，但客户端使用非插件方式
            // capabilities & CLIENT_SECURE_CONNECTION !=0 && capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA=0
            // 参见http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
            byte[] pass = PassSecure.nativeMysqlSecure(password, handshake.getAuthData());
            out.safeWrite(MysqlNumberUtils.writeNInt(pass.length, 1));
            out.safeWrite(pass);
        }

        if (withDbName()) {
            out.safeWrite(StringTool.safeConvertString2Bytes(dbName));
            out.write(0);
        }
        out.write(0);
        if ((capability & CapabilityFlagConst.CLIENT_CONNECT_ATTRS) != 0) {
            out.write(0);
            int pos = out.size();
            for (String key : this.propMap.keySet()) {
                byte[] tb = StringTool.safeConvertString2Bytes(key);
                out.safeWrite(MysqlNumberUtils.wirteLencodeLong(tb.length));
                out.safeWrite(tb);
                String v = this.propMap.get(key);
                tb = StringTool.safeConvertString2Bytes(v);
                out.safeWrite(MysqlNumberUtils.wirteLencodeLong(tb.length));
                out.safeWrite(tb);
            }

            out.setPosValue(pos - 1, (byte) (out.size() - pos));
        }
    }

    /**
     * 是否有dbname
     * 
     * @return true or false
     */
    private boolean withDbName() {
        return this.dbName != null && this.dbName.length() > 0;
    }

    public HandShakeV10 getHandshake() {
        return handshake;
    }

    public int getMaxSendPacketSize() {
        return maxSendPacketSize;
    }

    public int getCharSet() {
        return charSet;
    }

    public byte[] getReserved() {
        return reserved;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getDbName() {
        return dbName;
    }

    public Map<String, String> getPropMap() {
        return propMap;
    }

    public void setMaxSendPacketSize(int maxSendPacketSize) {
        this.maxSendPacketSize = maxSendPacketSize;
    }

    public void setCharSet(int charSet) {
        this.charSet = charSet;
    }

    public void setReserved(byte[] reserved) {
        this.reserved = reserved;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setPropMap(Map<String, String> propMap) {
        this.propMap = propMap;
    }
}
