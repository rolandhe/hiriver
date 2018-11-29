package com.hiriver.streamsource.impl;

import com.hiriver.streamsource.DbHostInfo;
import com.hiriver.streamsource.DbHostInfoSupplier;

/**
 * created by Yang Huawei (xander.yhw@alibaba-inc.com) on 2018/9/5 23:57
 */
public class StaticDbHostInfoSupplier implements DbHostInfoSupplier {

    private String hostUrl;
    private String userName;
    private String password;


    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHostUrl() {
        return hostUrl;
    }

    public void setHostUrl(String hostUrl) {
        this.hostUrl = hostUrl;
    }

    @Override
    public DbHostInfo available() {
        return new DbHostInfo() {
            @Override
            public String getHostUrl() {
                return hostUrl;
            }

            @Override
            public String getUserName() {
                return userName;
            }

            @Override
            public String getPassword() {
                return password;
            }
        };
    }
}
