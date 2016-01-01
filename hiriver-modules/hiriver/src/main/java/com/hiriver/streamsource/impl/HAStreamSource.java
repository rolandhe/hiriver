package com.hiriver.streamsource.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiriver.streamsource.StreamSource;
import com.hiriver.streamsource.StreamSourceSelector;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.GTidBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.ReadTimeoutExp;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

/**
 * 支持HA的{@link StreamSource}实现，它可以根据{@link StreamSourceSelector}策略选择一个物理数据源并使用它获取binlog数据。
 * 当与mysql断开重连后，它会先试图连接断开的数据源，失败后再连接其他数据源
 * 
 * @author hexiufeng
 *
 */
public class HAStreamSource implements StreamSource {
    private static final Logger LOG = LoggerFactory.getLogger(HAStreamSource.class);

    /**
     * 备选数据源
     */
    private List<StreamSource> haStreamSourceList;
    /**
     * 数据源选择策略
     */
    private StreamSourceSelector sourceSelector = new RandomStreamSourceSelector();

    /**
     * 记录当前使用的数据源
     */
    private StreamSource currentStreamSource;

    public List<StreamSource> getHaStreamSourceList() {
        return haStreamSourceList;
    }

    public void setHaStreamSourceList(List<StreamSource> haStreamSourceList) {
        this.haStreamSourceList = haStreamSourceList;
    }

    @Override
    public void openStream(BinlogPosition binlogPos) {
        if (null != tryOpenStream(binlogPos)) {
            return;
        }
        StreamSource source = sourceSelector.select(haStreamSourceList);
        source.openStream(ensureBinlogPosition(binlogPos));
        currentStreamSource = source;
        LOG.info("use another stream source {} success.", source.getHostUrl());
    }

    @Override
    public ValidBinlogOutput readValidInfo() throws ReadTimeoutExp {
        return currentStreamSource.readValidInfo();
    }

    /**
     * binlog file name + pos 模式下，当重新选择数据源时会发生变化
     * 
     * @param binlogPos BinlogPosition
     * @return BinlogPosition
     */
    private BinlogPosition ensureBinlogPosition(final BinlogPosition binlogPos) {
        if(binlogPos instanceof GTidBinlogPosition){
            return binlogPos;
        }
        throw new RuntimeException("it is not supported.");
    }

    /**
     * 试图使用当前的数据源
     * 
     * @param binlogPos 指定的同步点
     * @return 连接好的数据源
     */
    private StreamSource tryOpenStream(final BinlogPosition binlogPos) {
        if (currentStreamSource != null) {
            currentStreamSource.release();
            try {
                currentStreamSource.openStream(binlogPos);
                LOG.info("try reopen {} success.", currentStreamSource.getHostUrl());
            } catch (RuntimeException e) {
                LOG.info("try reopen {} failed.", currentStreamSource.getHostUrl());
                currentStreamSource = null;
            }
        }
        return currentStreamSource;
    }

    @Override
    public String getHostUrl() {
        return currentStreamSource == null ? "" : currentStreamSource.getHostUrl();
    }

    @Override
    public void release() {
        for (StreamSource source : this.haStreamSourceList) {
            source.release();
        }
    }

    @Override
    public boolean isOpen() {
        return this.currentStreamSource != null && this.currentStreamSource.isOpen();
    }

}
