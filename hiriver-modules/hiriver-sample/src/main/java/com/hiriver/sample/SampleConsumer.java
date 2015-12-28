package com.hiriver.sample;

import java.util.List;

import org.springframework.stereotype.Component;

import com.hiriver.channel.BinlogDataSet;
import com.hiriver.channel.stream.Consumer;
import com.hiriver.channel.stream.impl.AbstractConsumer;
import com.hiriver.unbiz.mysql.lib.output.BinlogColumnValue;
import com.hiriver.unbiz.mysql.lib.output.BinlogResultRow;
import com.hiriver.unbiz.mysql.lib.output.RowModifyTypeEnum;

@Component
public class SampleConsumer extends AbstractConsumer implements Consumer {

    @Override
    protected void consumerRowData(final BinlogDataSet rowData) {

        for (String tb : rowData.getRowDataMap().keySet()) {
            System.out.println("=======start table:" + tb+"=======");
            List<BinlogResultRow> rowList = rowData.getRowDataMap().get(tb);

            int index = 0;
            for (BinlogResultRow row : rowList) {
                System.out.println("=======start row [" + (index)+"]=======");
                if (row.getRowModifyType() == RowModifyTypeEnum.INSERT) {
                    outputRow(row.getAfterColumnValueList());
                }
                if (row.getRowModifyType() == RowModifyTypeEnum.DELETE) {
                    outputRow(row.getBeforeColumnValueList());
                }
                if (row.getRowModifyType() == RowModifyTypeEnum.UPDATE) {
                    outputRow(row.getBeforeColumnValueList());
                    outputRow(row.getAfterColumnValueList());
                }
                System.out.println("=======end row [" + (index)+"]=======");
                index++;
            }
            System.out.println("=======end table:" + tb + "=======");
        }

    }

    private void outputRow(List<BinlogColumnValue> rowValues) {
        for (BinlogColumnValue val : rowValues) {
            System.out.println(val);
        }
    }

}
