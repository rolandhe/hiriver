package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.TransportConfig;

public interface TableMetaProiverFactory {
    TableMetaProvider factory(String host, int port, String userName, String password,final TransportConfig transportConfig);
}
