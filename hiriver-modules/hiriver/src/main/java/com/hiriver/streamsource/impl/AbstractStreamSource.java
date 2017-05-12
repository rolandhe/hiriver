package com.hiriver.streamsource.impl;

import com.hiriver.streamsource.StreamSource;
import com.hiriver.unbiz.mysql.lib.TransportConfig;
import com.hiriver.unbiz.mysql.lib.filter.TableFilter;

/**
 * 抽象的数据源实现
 * 
 * @author hexiufeng
 *
 */
public abstract  class AbstractStreamSource implements StreamSource {
    /**
     * 通信属性
     */
    private TransportConfig transportConfig = new TransportConfig();
    private String userName;
    private String password;
    private String hostUrl;
    /**
     * 从库id，hiriver是从库
     */
    private int serverId;
    private int maxMaxPacketSize = 0;
    
    /**
     * 表过滤规则
     */
    private TableFilter tableFilter;
    
    public TransportConfig getTransportConfig() {
        return transportConfig;
    }
    public void setTransportConfig(TransportConfig transportConfig) {
        this.transportConfig = transportConfig;
    }
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
    public int getServerId() {
        return serverId;
    }
    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public TableFilter getTableFilter() {
        return tableFilter;
    }
    public int getMaxMaxPacketSize() {
        return maxMaxPacketSize;
    }
    public void setMaxMaxPacketSize(int maxMaxPacketSize) {
        this.maxMaxPacketSize = maxMaxPacketSize;
    }
    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }
    
    

}
