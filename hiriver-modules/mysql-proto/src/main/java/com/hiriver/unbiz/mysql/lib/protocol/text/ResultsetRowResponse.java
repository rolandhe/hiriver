package com.hiriver.unbiz.mysql.lib.protocol.text;

import java.util.ArrayList;
import java.util.List;

import com.hiriver.unbiz.mysql.lib.protocol.AbstractResponse;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.Response;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

/**
 * COM_QUERY指令返回结果中行数据部分描述
 * 
 * @author hexiufeng
 *
 */
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
        Position pos = Position.factory();
        for (int i = 0; i < columnList.size(); i++) {
            valueList.add(parseColumn(buf, i,pos));
        }
    }

    /**
     * 解析每一列的数据
     * 
     * @param buf 二进制数据
     * @param column 列的index
     * @return 列值
     */
    private ColumnValue parseColumn(byte[] buf, int column,Position pos) {
        
        if ((buf[pos.getPos()] & 0xff) == 0xfb) {
            pos.forwardPos();
            return null;
        } else {
            int lenc = (int) MysqlNumberUtils.readLencodeLong(buf, pos);
            byte[] tb = MysqlStringUtils.readFixString(buf, pos, lenc);
            return new ColumnValue(new TextColumnValueProvider(tb), columnList.get(column).toColumnDefinition());
        }

    }

}
