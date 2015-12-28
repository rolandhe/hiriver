package com.hiriver.streamsource.impl;

import com.hiriver.streamsource.StreamSource;
import com.hiriver.unbiz.mysql.lib.TransportConfig;
import com.hiriver.unbiz.mysql.lib.filter.TableFilter;

public abstract  class AbstractStreamSource implements StreamSource {
    private TransportConfig transportConfig = new TransportConfig();
    private String userName;
    private String password;
    private String hostUrl;
    private int serverId;
    private boolean checkSum = true;
    
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
    public boolean isCheckSum() {
        return checkSum;
    }

    public void setCheckSum(boolean checkSum) {
        this.checkSum = checkSum;
    }
    public TableFilter getTableFilter() {
        return tableFilter;
    }
    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }
    
    

}
