package com.hiriver.unbiz.mysql.lib;

import java.util.ArrayList;
import java.util.List;

/**
 * socket properties config:
 * <ul>
 * <li>connect timeout</li>
 * <li>soTimeout</li>
 * <li>receiveBufferSize</li>
 * <li>sendBufferSize</li>
 * <li>keepAlive</li>
 * </ul>
 * 
 * @author hexiufeng
 *
 */
public class TransportConfig {
    private int connectTimeout = 15000;
    private int soTimeout = 15000;
    private int receiveBufferSize;
    private int sendBufferSize;
    private boolean keepAlive = true;

    private List<String> initSqlList = new ArrayList<String>();

    {
        initSqlList.add("SET AUTOCOMMIT=1");
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public int getRecieveBufferSize() {
        return receiveBufferSize;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setRecieveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public List<String> getInitSqlList() {
        return initSqlList;
    }

    public void setInitSqlList(List<String> initSqlList) {
        this.initSqlList = initSqlList;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

}
