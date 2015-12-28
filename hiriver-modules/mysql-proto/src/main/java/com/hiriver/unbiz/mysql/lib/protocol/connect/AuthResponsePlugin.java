package com.hiriver.unbiz.mysql.lib.protocol.connect;

/**
 * 用于处理msyql权限验证响应的抽象描述。
 * 
 * <p>
 * mysql 5.1以后的版本，权限验由验证插件来完成，每一种插件有一个名字，比如mysql_native_password
 * </p>
 * 
 * @author hexiufeng
 *
 */
public interface AuthResponsePlugin {
    int getMustCapability(HandShakeV10 handshake);

    /**
     * 插件名称
     * 
     * @return string
     */
    String getPluginName();

    /**
     * 对password近打包
     * 
     * @param password password
     * @param handshake HandShakeV10,用于读取用于加密的随机数
     * @return 加密后的密码
     */
    byte[] generateAuthResponse(String password, HandShakeV10 handshake);
}
