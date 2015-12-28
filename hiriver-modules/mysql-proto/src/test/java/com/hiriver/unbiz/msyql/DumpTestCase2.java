package com.hiriver.unbiz.msyql;

import org.junit.Test;

import com.hiriver.unbiz.mysql.lib.BinlogStreamBlockingTransportImpl;
import com.hiriver.unbiz.mysql.lib.TransportConfig;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogFileBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidEventType;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.BaseRowEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.GTidEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.ReadTimeoutExp;

public class DumpTestCase2 {
    @Test
    public void testDump() {
        BinlogStreamBlockingTransportImpl tran = new BinlogStreamBlockingTransportImpl();
        tran.setHost("localhost");
        tran.setPort(3308);
        tran.setUserName("root");
        tran.setPassword("");
        tran.setServerId(599);
        // tran.open();
        tran.setTransportConfig(new TransportConfig());
        BinlogFileBinlogPosition pos = new BinlogFileBinlogPosition("mysql-bin.000001",174L);
        // GTidBinlogPosition pos = new
        // GTidBinlogPosition(GTSidTool.convertSidString2DumpFormatBytes("4b0c99b2-d2be-11e4-a66a-3c970ed9b743"),10L,65);
        tran.dump(pos);
        GTidEvent currentId = null;
        while (true) {
            try {
                ValidBinlogOutput value = tran.getBinlogOutput();
                if (value.isRowEvent()) {
                    BaseRowEvent row = (BaseRowEvent) value.getEvent();
                    System.out.print("data:");
                    System.out.println(row.getRowList().get(0).getAfterColumnValueList().get(1).getValue());
                } else if(value.getEventType() == ValidEventType.GTID) {
                    GTidEvent gi = (GTidEvent) value.getEvent();
                    currentId = gi;
                    System.out.print("gtid:");
                    System.out.println(gi.getGTidString());
                }
            } catch (ReadTimeoutExp e) {
                System.out.println("read again.");
                // if(currentId == null){
                // tran.dump(pos);
                // }else{
                // GTidBinlogPosition curpos = new
                // GTidBinlogPosition(GTSidTool.convertSidString2DumpFormatBytes(currentId.getGTidString()),currentId.getGno(),65);
                // tran.dump(curpos);
                // }
            }
        }
        // tran.close();
    }

    private byte[] convertMysqlUUID2bytes(String v) {
        String value = v.replaceAll("-", "");
        byte[] buffer = new byte[16];

        for (int i = 0; i < 16; i++) {
            String hex = value.substring(2 * i, 2 * i + 2);
            int intValue = Integer.parseInt(hex, 16);
            buffer[i] = (byte) intValue;
        }
        return buffer;
    }
}
