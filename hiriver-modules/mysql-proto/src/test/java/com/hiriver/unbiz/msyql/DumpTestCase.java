package com.hiriver.unbiz.msyql;

import org.junit.Test;

import com.hiriver.unbiz.mysql.lib.BinlogStreamBlockingTransportImpl;
import com.hiriver.unbiz.mysql.lib.TransportConfig;
import com.hiriver.unbiz.mysql.lib.filter.TableFilter;
import com.hiriver.unbiz.mysql.lib.output.BinlogResultRow;
import com.hiriver.unbiz.mysql.lib.output.RowModifyTypeEnum;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.GTIDSet;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.GTidBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.BaseRowEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.ReadTimeoutExp;

public class DumpTestCase {
    @Test
    public void testDump() {
        BinlogStreamBlockingTransportImpl tran = new BinlogStreamBlockingTransportImpl();
        tran.setHost("127.0.0.1");
        tran.setPort(3308);
        tran.setUserName("repl");
        tran.setPassword("pass123");
        tran.setServerId(55);
        tran.setCheckSum(true);
        tran.setTableFilter(new TableFilter(){

            @Override
            public boolean filter(String dbName, String tableName) {
                return dbName.equals("repl");
            }
            
        });
        // tran.open();
        tran.setTransportConfig(new TransportConfig());
        
//         BinlogFileBinlogPosition pos = new BinlogFileBinlogPosition("mysql-bin.000001",400L);
        GTIDSet gtidSet = new GTIDSet("8c80613e-ac5b-11e5-b170-148044d6636f:7");
        GTidBinlogPosition pos = new GTidBinlogPosition(gtidSet);
        
        tran.dump(pos);
        while (true) {
            try {
                ValidBinlogOutput value = tran.getBinlogOutput();
                if (value.isRowEvent()) {
                	BaseRowEvent row = (BaseRowEvent) value.getEvent();
                    System.out.print("data:");
                    BinlogResultRow r = row.getRowList().get(0);
                    if(r.getRowModifyType() == RowModifyTypeEnum.INSERT){
                    	System.out.println("insert");
                    	System.out.println(r.getAfterColumnValueList().get(0).getValue());
                    }
                    if(r.getRowModifyType() == RowModifyTypeEnum.DELETE){
                    	System.out.println("delete");
                    	System.out.println(r.getBeforeColumnValueList().get(0).getValue());
                    }
                    
                    if(r.getRowModifyType() == RowModifyTypeEnum.UPDATE){
                    	System.out.println("update");
                    	System.out.println(r.getBeforeColumnValueList().get(0).getValue());
                    	System.out.println(r.getAfterColumnValueList().get(0).getValue());
                    }
                } 
//                else if(value.getEventType() == ValidEventType.GTID){
//                    GTidEvent gi = (GTidEvent) value.getEvent();
//                    gtidSet.updateGTIDPoint(gi.getSidString(), gi.getGno());
//                    System.out.print("gtid:");
//                    System.out.println(gi.getGTidString());
//                }
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
