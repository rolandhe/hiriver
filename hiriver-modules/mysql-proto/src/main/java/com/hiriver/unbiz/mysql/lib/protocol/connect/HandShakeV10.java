package com.hiriver.unbiz.mysql.lib.protocol.connect;

import com.hiriver.unbiz.mysql.lib.protocol.*;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.PacketTool;

/**
 * Initial Handshake Packet。<br>
 * 
 * <a href="http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake"> http://dev.
 * mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake </a>
 * 
 * @author hexiufeng
 *
 */
public class HandShakeV10 extends AbstractResponse implements Response {

    private int protocolVersion;
    private String serverVer;
    private long connectionId;
    private byte[] authPart1;
    private int filler;
    private int capabilityLow;

    private int charset;
    private int status;

    private int capabilityHigh;

    private int capability;
    private int authDataLen;

    private byte[] reserved;
    private byte[] authPart2;

    private String authPluginName;

    public HandShakeV10() {
    }

    @Override
    public void parse(byte[] buf) {
        Position pos = Position.factory();
        if(PacketTool.isErrPackete(buf)){
            ERRPacket ep = new ERRPacket();
            ep.parse(buf);
            throw new RuntimeException("handshake error:" + ep.getErrorMessage());
        }
        this.protocolVersion = MysqlNumberUtils.read1Int(buf, pos);
        byte[] sv = MysqlStringUtils.readNulString(buf, pos);
        serverVer = new String(sv);

        connectionId = MysqlNumberUtils.readUnsignedInt(buf, pos);

        authPart1 = MysqlStringUtils.readFixString(buf, pos, 8);

        filler = MysqlNumberUtils.read1Int(buf, pos);

        capabilityLow = MysqlNumberUtils.read2Int(buf, pos);

        if (buf.length == pos.getPos()) {
            return;
        }

        charset = MysqlNumberUtils.read1Int(buf, pos);

        status = MysqlNumberUtils.read2Int(buf, pos);

        capabilityHigh = MysqlNumberUtils.read2Int(buf, pos);

        capability = (capabilityHigh << 16) | capabilityLow;

        int l = MysqlNumberUtils.read1Int(buf, pos);
        if (isNeedAuth()) {
            authDataLen = l;
        }

        reserved = MysqlStringUtils.readFixString(buf, pos, 10);

        if (isSecureConnect()) {
            int len = Math.max(13, authDataLen - 8);
            authPart2 = MysqlStringUtils.readFixString(buf, pos, len);
        }
        if (isNeedAuth()) {
            // Bug#59453
            if (buf[buf.length - 1] != 0) {
                authPluginName = new String(MysqlStringUtils.readFixString(buf, pos, buf.length - pos.getPos()));
            } else {
                authPluginName = new String(MysqlStringUtils.readNulString(buf, pos));
            }
        }
    }

    /**
     * 获取加密密码的随机串，20bytes
     * 
     * @return
     */
    public byte[] getAuthData() {
        if (isSecureConnect()) {
            byte[] authData = new byte[20];
            System.arraycopy(authPart1, 0, authData, 0, 8);
            System.arraycopy(authPart2, 0, authData, 8, 12);
            return authData;
        }
        return this.authPart1;
    }

    public boolean isSecureConnect() {
        return hasCapability(CapabilityFlagConst.CLIENT_SECURE_CONNECTION);
    }

    public boolean hasConnectAttr() {
        return hasCapability(CapabilityFlagConst.CLIENT_CONNECT_ATTRS);
    }

    public boolean isNeedAuth() {
        return hasCapability(CapabilityFlagConst.CLIENT_PLUGIN_AUTH);
    }

    public boolean hasCapability(int cap) {
        return (capability & cap) != 0;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public String getServerVer() {
        return serverVer;
    }

    public long getConnectionId() {
        return connectionId;
    }

    public byte[] getAuthPart1() {
        return authPart1;
    }

    public int getFiller() {
        return filler;
    }

    public int getCapabilityLow() {
        return capabilityLow;
    }

    public int getCharset() {
        return charset;
    }

    public int getStatus() {
        return status;
    }

    public int getCapabilityHigh() {
        return capabilityHigh;
    }

    public int getCapability() {
        return capability;
    }

    public int getAuthDataLen() {
        return authDataLen;
    }

    public byte[] getReserved() {
        return reserved;
    }

    public byte[] getAuthPart2() {
        return authPart2;
    }

    public String getAuthPluginName() {
        return authPluginName;
    }
}
