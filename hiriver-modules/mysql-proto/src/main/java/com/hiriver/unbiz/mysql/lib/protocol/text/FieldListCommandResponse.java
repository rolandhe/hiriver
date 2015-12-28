package com.hiriver.unbiz.mysql.lib.protocol.text;

import java.util.ArrayList;
import java.util.List;

import com.hiriver.unbiz.mysql.lib.ResultContentReader;
import com.hiriver.unbiz.mysql.lib.protocol.AbstractResponse;
import com.hiriver.unbiz.mysql.lib.protocol.Response;
import com.hiriver.unbiz.mysql.lib.protocol.tool.PacketTool;

public class FieldListCommandResponse extends AbstractResponse implements Response {
    private List<ColumnDefinitionResponse> columnList = new ArrayList<ColumnDefinitionResponse>(32);

    private final ResultContentReader resultContextReader;

    public FieldListCommandResponse(ResultContentReader resultContextReader) {
        this.resultContextReader = resultContextReader;
    }

    @Override
    public void parse(byte[] buf) {
        ColumnDefinitionResponse colDef = new ColumnDefinitionResponse(false);
        colDef.parse(buf);
        columnList.add(colDef);
        byte[] nextBuffer = resultContextReader.readNextPacketPayload();
        while (!PacketTool.isEofPackete(nextBuffer)) {
            ColumnDefinitionResponse nextColDef = new ColumnDefinitionResponse(false);
            nextColDef.parse(nextBuffer);
            columnList.add(nextColDef);
            nextBuffer = resultContextReader.readNextPacketPayload();
        }
    }

    public List<ColumnDefinitionResponse> getColumnList() {
        return columnList;
    }

}
