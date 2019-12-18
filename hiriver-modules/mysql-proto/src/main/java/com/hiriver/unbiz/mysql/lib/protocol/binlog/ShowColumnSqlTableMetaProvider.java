package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.CharsetMapping;
import com.hiriver.unbiz.mysql.lib.ColumnType;
import com.hiriver.unbiz.mysql.lib.TextProtocolBlockingTransport;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.TableMapEvent;
import com.hiriver.unbiz.mysql.lib.protocol.text.ColumnValue;
import com.hiriver.unbiz.mysql.lib.protocol.text.ResultsetRowResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryResponse;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public abstract class ShowColumnSqlTableMetaProvider extends AbstractTableMetaProvider implements TableMetaProvider {
    @Override
    protected List<ColumnDefinition> readMeta(String tableName, TableMapEvent tableMapEvent, TextProtocolBlockingTransport textTrans) {
        List<ColumnDefinition> columnDefinitionList = new ArrayList<>();
        String sql = "show full columns from " + tableName;
        TextCommandQueryResponse response = textTrans.execute(sql);
        if(response.getRowList() == null || response.getRowList().size() == 0){
            return columnDefinitionList;
        }
        for (ResultsetRowResponse row  : response.getRowList()) {
            parseColumnDefinition(tableMapEvent.hasEnumOrSet(), columnDefinitionList, row);
        }
        return columnDefinitionList;
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
        if(collation == null||collation.length() == 0) {
            columnDefinition.setCharset(null);
        } else {
            String cs = CharsetMapping.getJavaEncodingForCollation(collation);
            columnDefinition.setCharset(cs);
        }
        String key = getColumnStringValue(row,4);
        if(!StringUtils.isEmpty(key)) {
;           if(key.equalsIgnoreCase("PRI")) {
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

//    String name = row.getValueList().get(0).getValueAsString();
//    String value = row.getValueList().get(1).getValueAsString();
//            LOGGER.info("enum or set of {}.{}:{}",tableName,name,value);
//            if(value.startsWith("enum(")){
//        String enumValue = value.substring("enum(".length(),value.length() - 1);
//        map.get(name).getEnumList().addAll(parseEnumOrSetValue(enumValue));
//    }
//            if(value.startsWith("set(")){
//        String setValue = value.substring("set(".length(),value.length() - 1);
//        map.get(name).getSetList().addAll(parseEnumOrSetValue(setValue));
//    }

}
