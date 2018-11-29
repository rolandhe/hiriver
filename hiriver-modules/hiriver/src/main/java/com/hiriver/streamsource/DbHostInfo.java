package com.hiriver.streamsource;

/**
 * <pre>
 * 数据库连接信息
 * </pre>
 * created by Yang Huawei (xander.yhw@alibaba-inc.com) on 2018/9/3 18:05
 */
public interface DbHostInfo {

    String getHostUrl();

    String getUserName();

    String getPassword();

}
