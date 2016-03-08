package com.hiriver.sample;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.hiriver.channel.BinlogDataSet;
import com.hiriver.channel.stream.Consumer;
import com.hiriver.channel.stream.impl.AbstractConsumer;
import com.hiriver.unbiz.mysql.lib.output.BinlogColumnValue;
import com.hiriver.unbiz.mysql.lib.output.BinlogResultRow;
import com.hiriver.unbiz.mysql.lib.output.RowModifyTypeEnum;

@Component
public class SampleConsumer extends AbstractConsumer implements Consumer {
    private static final Logger LOG = LoggerFactory.getLogger(SampleConsumer.class);

    @Override
    protected void consumeRowData(final BinlogDataSet rowData) {

        for (String tb : rowData.getRowDataMap().keySet()) {
            LOG.info("=======start table:" + tb + "=======");
            List<BinlogResultRow> rowList = rowData.getRowDataMap().get(tb);

            int index = 0;
            for (BinlogResultRow row : rowList) {
                LOG.info("=======start row [" + (index) + "]=======");
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
                LOG.info("=======end row [" + (index) + "]=======");
                index++;
            }
            LOG.info("=======end table:" + tb + "=======");
        }
    }

    private void outputRow(List<BinlogColumnValue> rowValues) {
        for (BinlogColumnValue val : rowValues) {
            LOG.info(val.toString());
        }
    }

}
