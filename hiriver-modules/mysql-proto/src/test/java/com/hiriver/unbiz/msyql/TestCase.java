package com.hiriver.unbiz.msyql;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.hiriver.unbiz.mysql.lib.ColumnType;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.text.ColumnValue;
import com.hiriver.unbiz.mysql.lib.protocol.text.ResultsetRowResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryResponse;
import org.apache.commons.lang3.StringUtils;
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
        String[] array = "1-2".split("-");
        System.out.println(UUID.randomUUID().toString());
        TransportConfig conf = new TransportConfig();
        TextProtocolBlockingTransportImpl transport =
                new TextProtocolBlockingTransportImpl("localhost", 3309, "root", "123456", "repl");
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

    @Test
    public void test2() {

        TransportConfig conf = new TransportConfig();
        TextProtocolBlockingTransportImpl transport =
                new TextProtocolBlockingTransportImpl("localhost", 3309, "root", "123456", "cat");
        transport.setTransportConfig(conf);

        transport.open();
        TextCommandQueryResponse textCommandQueryResponse = transport.execute("SHOW FULL COLUMNS from demo");
        List<ColumnDefinition> columnDefinitionList = new ArrayList<>();
        for(ResultsetRowResponse resultsetRowResponse:textCommandQueryResponse.getRowList()) {
            parseColumnDefinition(false,columnDefinitionList,resultsetRowResponse);
        }
        transport.close();
    }

    private void parseColumnDefinition(boolean hasEnumOrSet, List<ColumnDefinition> columnDefinitionList, ResultsetRowResponse row) {
        ColumnDefinition columnDefinition = new ColumnDefinition();
        columnDefinition.setColumName(row.getValueList().get(0).getValueAsString());
        String colType = row.getValueList().get(1).getValueAsString();
        String[] ar = StringUtils.split(colType," ");
        if(ar.length == 2 && "unsigned".equals(ar[1])) {
            columnDefinition.setUnsigned(true);
        } else {
            columnDefinition.setUnsigned(false);
        }
        columnDefinition.setType(parseColumnType(ar[0]));

        if(hasEnumOrSet) {
            if(colType.startsWith("enum(")){
                String enumValue = colType.substring("enum(".length(),colType.length() - 1);
                columnDefinition.getEnumList().addAll(parseEnumOrSetValue(enumValue));
            } else if(colType.startsWith("set(")){
                String setValue = colType.substring("set(".length(),colType.length() - 1);
                columnDefinition.getSetList().addAll(parseEnumOrSetValue(setValue));
            }
        }

        String collation = getColumnStringValue(row,2);
        columnDefinition.setCharset(StringUtils.isEmpty(collation)?null:StringUtils.split(collation,"_")[0]);
        String key = getColumnStringValue(row,4);
        if(!StringUtils.isEmpty(key)) {
            if(key.equalsIgnoreCase("PRI")) {
                columnDefinition.setPrimary(true);
            } else if (key.equalsIgnoreCase("UNI")) {
                columnDefinition.setUnique(true);
            } else if(key.equalsIgnoreCase("MUL")) {
                columnDefinition.setKey(true);
            }
        }
        columnDefinitionList.add(columnDefinition);
    }

    private String getColumnStringValue(ResultsetRowResponse row,int index) {
        ColumnValue columnValue = row.getValueList().get(index);
        if(columnValue == null) {
            return null;
        }
        return columnValue.getValueAsString();
    }

    private ColumnType parseColumnType(String type) {
        int pos =  type.indexOf("(");
        if(pos >=0) {
            return ColumnType.ofTypeName(type.substring(0,pos));
        }
        return ColumnType.ofTypeName(type);
    }

    private List<String> parseEnumOrSetValue(String value){
        String[] array = value.split(",");
        List<String> list = new ArrayList<>(array.length);
        for(String v : array){
            list.add(v.substring(1,v.length() - 1));
        }
        return list;
    }
}
