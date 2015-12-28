package com.hiriver.unbiz.mysql.lib.protocol.connect;

import java.util.HashMap;
import java.util.Map;

/**
 * AuthResponsePlugin的类工厂，目前只实现MysqlNativeAuthResponsePlugin
 * 
 * @author hexiufeng
 *
 */
public class AuthResponsePluginFactory {
    private AuthResponsePluginFactory() {
    }

    private static final Map<String, AuthResponsePlugin> AUTH_CACHE = new HashMap<String, AuthResponsePlugin>();

    static {
        register(new MysqlNativeAuthResponsePlugin());
    }

    /**
     * 根据验证插件名获取AuthResponsePlugin
     * 
     * @param authPluginName 插件名
     * @return AuthResponsePlugin
     */
    public static AuthResponsePlugin factory(String authPluginName) {
        return AUTH_CACHE.get(authPluginName);
    }

    /**
     * 注册AuthResponsePlugin
     * 
     * @param plug AuthResponsePlugin
     */
    private static void register(AuthResponsePlugin plug) {
        AUTH_CACHE.put(plug.getPluginName(), plug);
    }
}
