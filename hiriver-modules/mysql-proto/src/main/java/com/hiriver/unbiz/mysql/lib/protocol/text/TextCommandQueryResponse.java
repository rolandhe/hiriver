package com.hiriver.unbiz.mysql.lib.protocol.text;

import java.util.ArrayList;
import java.util.List;

import com.hiriver.unbiz.mysql.lib.ResultContentReader;
import com.hiriver.unbiz.mysql.lib.protocol.AbstractResponse;
import com.hiriver.unbiz.mysql.lib.protocol.EOFPacket;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.Response;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.PacketTool;

/**
 * COM_QUERY指令的返回结果
 * 
 * @author hexiufeng
 *
 */
public class TextCommandQueryResponse extends AbstractResponse implements Response {
    private int columnCount;
    private List<ColumnDefinitionResponse> columnList = new ArrayList<ColumnDefinitionResponse>(32);
    private List<ResultsetRowResponse> rowList = new ArrayList<ResultsetRowResponse>();

    private final ResultContentReader resultContextReader;

    public TextCommandQueryResponse(ResultContentReader resultContextReader) {
        this.resultContextReader = resultContextReader;
    }

    @Override
    public void parse(byte[] buf) {
        Position pos = Position.factory();
        columnCount = (int) MysqlNumberUtils.readLencodeLong(buf, pos);
        readColumnDefinition(columnCount);

        byte[] rowBuffer = resultContextReader.readNextPacketPayload();
        while (!PacketTool.isEofPacket(rowBuffer, 0)) {
            // pasre row
            ResultsetRowResponse row = new ResultsetRowResponse(columnList);
            row.parse(rowBuffer);
            rowList.add(row);
            rowBuffer = resultContextReader.readNextPacketPayload();
        }
    }

    private void readColumnDefinition(int count) {
        for (int i = 0; i < count; i++) {
            ColumnDefinitionResponse colDef = new ColumnDefinitionResponse(true);
            colDef.parse(resultContextReader.readNextPacketPayload());

            columnList.add(colDef);
        }
        // parse eof
        EOFPacket eof = new EOFPacket();
        eof.parse(resultContextReader.readNextPacketPayload());
    }

    public List<ColumnDefinitionResponse> getColumnList() {
        return columnList;
    }

    public List<ResultsetRowResponse> getRowList() {
        return rowList;
    }
}
