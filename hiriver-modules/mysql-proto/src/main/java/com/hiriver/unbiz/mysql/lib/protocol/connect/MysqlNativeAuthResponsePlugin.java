package com.hiriver.unbiz.mysql.lib.protocol.connect;

import com.hiriver.unbiz.mysql.lib.protocol.CapabilityFlagConst;
import com.hiriver.unbiz.mysql.lib.protocol.tool.PassSecure;

/**
 * mysql_native_password加密密码实现
 * 
 * @author hexiufeng
 *
 */
class MysqlNativeAuthResponsePlugin implements AuthResponsePlugin {

    @Override
    public String getPluginName() {
        return "mysql_native_password";
    }

    @Override
    public byte[] generateAuthResponse(String password, HandShakeV10 handshake) {
        return PassSecure.nativeMysqlSecure(password, handshake.getAuthData());
    }

    @Override
    public int getMustCapability(HandShakeV10 handshake) {
        int capability = CapabilityFlagConst.CLIENT_PLUGIN_AUTH | CapabilityFlagConst.CLIENT_LONG_PASSWORD
                | CapabilityFlagConst.CLIENT_PROTOCOL_41 | CapabilityFlagConst.CLIENT_TRANSACTIONS
                | CapabilityFlagConst.CLIENT_MULTI_RESULTS | CapabilityFlagConst.CLIENT_SECURE_CONNECTION;
        if (handshake.hasCapability(CapabilityFlagConst.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)) {
            capability |= CapabilityFlagConst.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
        }

        return capability;
    }

}
