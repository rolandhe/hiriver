package com.hiriver.channel.stream.impl;

import com.hiriver.channel.BinlogDataSet;
import com.hiriver.channel.stream.BufferableBinlogDataSet;
import com.hiriver.unbiz.mysql.lib.output.BinlogResultRow;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * created by Yang Huawei (xander.yhw@alibaba-inc.com) on 2018/10/20 11:40
 */
public class LimitByRowsChannelBufferTest {

    @Test
    public void testNormalCase() {
        LimitByRowsChannelBuffer limitByRowsChannelBuffer = new LimitByRowsChannelBuffer(2);

        BinlogDataSet binlogDataSet =
                new BinlogDataSet("channelId", "sourceHostUrl", "gtId", "binlogPos");
        List<BinlogResultRow> binlogResultRows = new ArrayList<>();
        binlogResultRows.add(new BinlogResultRow(null, null, null, 0));
        binlogDataSet.getRowDataMap().put("tableA", binlogResultRows);

        BufferableBinlogDataSet bufferableBinlogDataSet =
                new DefaultBufferableBinlogDataSet(binlogDataSet);
        Assert.assertTrue(
                limitByRowsChannelBuffer.push(bufferableBinlogDataSet, 1, TimeUnit.NANOSECONDS));
        Assert.assertTrue(
                limitByRowsChannelBuffer.push(bufferableBinlogDataSet, 1, TimeUnit.NANOSECONDS));
        Assert.assertFalse(
                limitByRowsChannelBuffer.push(bufferableBinlogDataSet, 1, TimeUnit.NANOSECONDS));
    }

    @Test
    public void testBinlogDataSetSizeChange() {
        LimitByRowsChannelBuffer limitByRowsChannelBuffer = new LimitByRowsChannelBuffer(2);

        BinlogDataSet binlogDataSet =
                new BinlogDataSet("channelId", "sourceHostUrl", "gtId", "binlogPos");
        List<BinlogResultRow> binlogResultRows = new ArrayList<>();
        binlogResultRows.add(new BinlogResultRow(null, null, null, 0));
        binlogDataSet.getRowDataMap().put("tableA", binlogResultRows);

        BufferableBinlogDataSet bufferableBinlogDataSet =
                new DefaultBufferableBinlogDataSet(binlogDataSet);
        Assert.assertTrue(
                limitByRowsChannelBuffer.push(bufferableBinlogDataSet, 1, TimeUnit.NANOSECONDS));
        Assert.assertTrue(limitByRowsChannelBuffer.availablePermits() == 1);
        binlogResultRows.add(new BinlogResultRow(null, null, null, 0));
        Assert.assertTrue(limitByRowsChannelBuffer.availablePermits() == 1);
        Assert.assertTrue(limitByRowsChannelBuffer.pop(1, TimeUnit.NANOSECONDS) != null);
        Assert.assertTrue(limitByRowsChannelBuffer.availablePermits() == 2);

    }


    @Test
    public void testBinlogDataSetSizeBiggerThanTotalPermit() {
        LimitByRowsChannelBuffer limitByRowsChannelBuffer = new LimitByRowsChannelBuffer(2);

        BinlogDataSet overSizeBinlogDataSet =
                new BinlogDataSet("channelId", "sourceHostUrl", "gtId", "binlogPos");
        List<BinlogResultRow> binlogResultRows = new ArrayList<>();
        binlogResultRows.add(new BinlogResultRow(null, null, null, 0));
        binlogResultRows.add(new BinlogResultRow(null, null, null, 0));
        binlogResultRows.add(new BinlogResultRow(null, null, null, 0));
        overSizeBinlogDataSet.getRowDataMap().put("tableA", binlogResultRows);


        Assert.assertTrue(limitByRowsChannelBuffer.availablePermits() == 2);
        Assert.assertTrue(limitByRowsChannelBuffer
                .push(new DefaultBufferableBinlogDataSet(overSizeBinlogDataSet), 1, TimeUnit.NANOSECONDS));
        Assert.assertTrue(limitByRowsChannelBuffer.availablePermits() == 0);
        Assert.assertTrue(limitByRowsChannelBuffer.pop(1, TimeUnit.NANOSECONDS) != null);
        Assert.assertTrue(limitByRowsChannelBuffer.availablePermits() == 2);

        BinlogDataSet smallSizeBinlogDataSet =
                new BinlogDataSet("channelId", "sourceHostUrl", "gtId", "binlogPos");
        List<BinlogResultRow> oneRow = new ArrayList<>();
        oneRow.add(new BinlogResultRow(null, null, null, 0));
        smallSizeBinlogDataSet.getRowDataMap().put("tableA", oneRow);

        Assert.assertTrue(limitByRowsChannelBuffer
                .push(new DefaultBufferableBinlogDataSet(smallSizeBinlogDataSet), 1, TimeUnit.NANOSECONDS));

        Assert.assertFalse(limitByRowsChannelBuffer
                .push(new DefaultBufferableBinlogDataSet(overSizeBinlogDataSet), 1, TimeUnit.NANOSECONDS));
        Assert.assertTrue(limitByRowsChannelBuffer.availablePermits() == 1);
        Assert.assertTrue(limitByRowsChannelBuffer.pop(1, TimeUnit.NANOSECONDS) != null);
        Assert.assertTrue(limitByRowsChannelBuffer.availablePermits() == 2);

        Assert.assertTrue(limitByRowsChannelBuffer
                .push(new DefaultBufferableBinlogDataSet(overSizeBinlogDataSet), 1, TimeUnit.NANOSECONDS));

    }

}
