package com.hiriver.unbiz.mysql.lib.protocol.text;

import java.util.ArrayList;
import java.util.List;

import com.hiriver.unbiz.mysql.lib.protocol.AbstractResponse;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.Response;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

public class ResultsetRowResponse extends AbstractResponse implements Response {
    private List<ColumnValue> valueList;
    private final List<ColumnDefinitionResponse> columnList;

    public List<ColumnValue> getValueList() {
        return valueList;
    }

    public ResultsetRowResponse(List<ColumnDefinitionResponse> columnList) {
        this.columnList = columnList;
        valueList = new ArrayList<ColumnValue>(columnList.size());
    }

    @Override
    public void parse(byte[] buf) {
        for (int i = 0; i < columnList.size(); i++) {
            valueList.add(parseColumn(buf, i));
        }
    }

    private ColumnValue parseColumn(byte[] buf, int column) {
        Position pos = Position.factory();
        if ((buf[pos.getPos()] & 0xff) == 0xfb) {
            return null;
        } else {
            int lenc = (int) MysqlNumberUtils.readLencodeLong(buf, pos);
            byte[] tb = MysqlStringUtils.readFixString(buf, pos, lenc);
            return new ColumnValue(new TextColumnValueProvider(tb), columnList.get(column).toColumnDefinition());
        }

    }

}
