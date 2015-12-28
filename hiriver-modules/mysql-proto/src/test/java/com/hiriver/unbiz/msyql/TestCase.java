package com.hiriver.unbiz.msyql;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.hiriver.unbiz.mysql.lib.TextProtocolBlockingTransportImpl;
import com.hiriver.unbiz.mysql.lib.TransportConfig;
import com.hiriver.unbiz.mysql.lib.protocol.text.ColumnDefinitionResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.FieldListCommandResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandFieldListRequest;



public class TestCase {
    @Test
    public void test() {
        System.out.println(UUID.randomUUID().toString());
        TransportConfig conf = new TransportConfig();
        TextProtocolBlockingTransportImpl transport =
                new TextProtocolBlockingTransportImpl("localhost", 3320, "root", "", "repl");
        transport.setTransportConfig(conf);
        TextCommandFieldListRequest fieldListRequest = new TextCommandFieldListRequest("test");
        transport.open();
        FieldListCommandResponse resp = transport.showFieldList(fieldListRequest);
        for (ColumnDefinitionResponse def : resp.getColumnList()) {
            System.out.println(def.isPrimayKey());
        }
        transport.close();
        Assert.assertTrue(resp.getColumnList().size() == 2);
    }
}
