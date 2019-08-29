package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.TransportConfig;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.text.ColumnDefinitionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 指定数据库的字符集
 *
 */
public class ConfCharsetTableMetaProiverFactory implements TableMetaProiverFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfCharsetTableMetaProiverFactory.class);

    private final String confCharset;

    public ConfCharsetTableMetaProiverFactory(String confCharset) {
        this.confCharset = confCharset;
    }


    @Override
    public TableMetaProvider factory(final String host, final int port, final String userName, final String password,final TransportConfig transportConfig) {
        return new ShowColumnListCommandTableMetaProvider() {

            @Override
            protected ColumnDefinition createColumnDefinition(ColumnDefinitionResponse coldef) {
                ColumnDefinition definition =  super.createColumnDefinition(coldef);
                definition.setCharset(confCharset);
                return definition;
            }

            @Override
            protected String getHost() {
                return host;
            }

            @Override
            protected int getPort() {
                return port;
            }

            @Override
            protected String getUserName() {
                return userName;
            }

            @Override
            protected String getPassword() {
                return password;
            }

            @Override
            protected TransportConfig getTransportConfig() {
                return transportConfig;
            }
        };
    }


}
