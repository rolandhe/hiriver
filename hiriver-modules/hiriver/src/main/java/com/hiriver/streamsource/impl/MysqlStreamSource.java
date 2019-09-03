package com.hiriver.streamsource.impl;

import com.hiriver.streamsource.StreamSource;
import com.hiriver.unbiz.mysql.lib.BinlogStreamBlockingTransport;
import com.hiriver.unbiz.mysql.lib.BinlogStreamBlockingTransportImpl;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.ReadTimeoutExp;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * 针对物理mysql数据源的{@link StreamSource}实现。内部委托给
 * {@link BinlogStreamBlockingTransportImpl}实现
 * 
 * @author hexiufeng
 *
 */
public class MysqlStreamSource extends AbstractStreamSource implements StreamSource {
    private BinlogStreamBlockingTransport transport;
    private boolean opened = false;


    /**
     * 初始化BinlogStreamBlockingTransportImpl
     */
    private void initTransport() {
        release();
        String[] array = super.getHostUrl().split(":");
        int port = Integer.parseInt(array[1]);
        BinlogStreamBlockingTransportImpl t =  new BinlogStreamBlockingTransportImpl(array[0],port,super.getUserName(),super.getPassword());
        t.setServerId(super.getServerId());
        t.setTransportConfig(super.getTransportConfig());
        t.setTableFilter(getTableFilter());
        t.setMaxMaxPacketSize(super.getMaxMaxPacketSize());
        transport = t;
    }

    @Override
    public void openStream(BinlogPosition binlogPos) {
        if(opened){
            return ;
        }
        initTransport();
        transport.dump(binlogPos);
        opened = true;
    }

    @Override
    public ValidBinlogOutput readValidInfo() throws ReadTimeoutExp {
        return transport.getBinlogOutputImmediately();
    }

    @Override
    public void release(){
        if(transport != null){
            transport.close();
            transport = null;
        }
        opened = false;
    }

    @Override
    public boolean isOpen() {
        return opened;
    }

}
