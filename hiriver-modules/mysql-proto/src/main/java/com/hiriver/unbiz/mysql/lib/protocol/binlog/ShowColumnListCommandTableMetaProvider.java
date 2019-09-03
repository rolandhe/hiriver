package com.hiriver.unbiz.mysql.lib.protocol.binlog;

import com.hiriver.unbiz.mysql.lib.TextProtocolBlockingTransport;
import com.hiriver.unbiz.mysql.lib.TransportConfig;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.TableMapEvent;
import com.hiriver.unbiz.mysql.lib.protocol.text.ColumnDefinitionResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.FieldListCommandResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.ResultsetRowResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandFieldListRequest;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ShowColumnListCommandTableMetaProvider extends AbstractTableMetaProvider implements TableMetaProvider {

    protected List<ColumnDefinition> readMeta(String tableName, TableMapEvent tableMapEvent, TextProtocolBlockingTransport textTrans) {
        List<ColumnDefinition> list = readFieldListMeta(tableName, textTrans);
        if(tableMapEvent.hasEnumOrSet()){
            Map<String, ColumnDefinition> map = new HashMap<>();
            for(ColumnDefinition definition : list){
                map.put(definition.getColumName(),definition);
            }
            readEnumOrSetMeta(tableName, map, textTrans);
        }
        return list;
    }
    /**
     *
     * 读取表字段元数据
     *
     * @param tableName
     * @param textTrans
     */
    private List<ColumnDefinition>  readFieldListMeta(String tableName,  TextProtocolBlockingTransport textTrans) {
        List<ColumnDefinition> list = new ArrayList<>();
        TextCommandFieldListRequest request = new TextCommandFieldListRequest(tableName);
        FieldListCommandResponse response = textTrans.showFieldList(request);
        for (ColumnDefinitionResponse coldef : response.getColumnList()) {
            ColumnDefinition def = createColumnDefinition(coldef);
            list.add(def);
        }
        return list;
    }

    /**
     * 读取enum 或者set类型的元数据，通过show columns sql命令读取类型，然后解析
     *
     * @param tableName
     * @param map
     * @param textTrans
     */
    private void readEnumOrSetMeta(String tableName, Map<String,ColumnDefinition> map, TextProtocolBlockingTransport textTrans) {
        String sql = "show columns from " + tableName + " where Type like 'set(%' or Type like 'enum(%'";
        TextCommandQueryResponse response = textTrans.execute(sql);
        if(response.getRowList() == null || response.getRowList().size() == 0){
            return;
        }
        for (ResultsetRowResponse row  : response.getRowList()) {
            String name = row.getValueList().get(0).getValueAsString();
            String value = row.getValueList().get(1).getValueAsString();
            LOGGER.info("enum or set of {}.{}:{}",tableName,name,value);
            if(value.startsWith("enum(")){
                String enumValue = value.substring("enum(".length(),value.length() - 1);
                map.get(name).getEnumList().addAll(parseEnumOrSetValue(enumValue));
            }
            if(value.startsWith("set(")){
                String setValue = value.substring("set(".length(),value.length() - 1);
                map.get(name).getSetList().addAll(parseEnumOrSetValue(setValue));
            }
        }
    }
    private List<String> parseEnumOrSetValue(String value){
        String[] array = value.split(",");
        List<String> list = new ArrayList<>(array.length);
        for(String v : array){
            list.add(v.substring(1,v.length() - 1));
        }
        return list;
    }


    /**
     * 从{@link ColumnDefinitionResponse}转换成外部可以识别的 {@link ColumnDefinition}
     *
     * @param coldef ColumnDefinitionResponse定义
     * @return ColumnDefinition
     */
    protected  ColumnDefinition createColumnDefinition(ColumnDefinitionResponse coldef){
        ColumnDefinition def = new ColumnDefinition();
        def.setColumName(coldef.getName());
        def.setCharset(coldef.getCharset());
        def.setKey(coldef.isKey());
        def.setPrimary(coldef.isPrimayKey());
        def.setType(coldef.getType());
        def.setUnique(coldef.isUniqueKey());
        def.setUnsigned(coldef.isUnsigned());
        def.setLen(coldef.getColumnLength());
        return def;
    }
    protected abstract String getHost();
    protected abstract int getPort();
    protected abstract String getUserName();
    protected abstract String getPassword();
    protected abstract TransportConfig getTransportConfig();
}
