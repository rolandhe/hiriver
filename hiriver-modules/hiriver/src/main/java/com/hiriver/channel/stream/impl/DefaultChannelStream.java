package com.hiriver.channel.stream.impl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiriver.channel.BinlogDataSet;
import com.hiriver.channel.stream.BinlogPositionStoreTrigger;
import com.hiriver.channel.stream.BufferableBinlogDataSet;
import com.hiriver.channel.stream.ChannelBuffer;
import com.hiriver.channel.stream.ChannelStream;
import com.hiriver.channel.stream.Consumer;
import com.hiriver.channel.stream.TransactionRecognizer;
import com.hiriver.position.store.BinlogPositionStore;
import com.hiriver.streamsource.StreamSource;
import com.hiriver.unbiz.mysql.lib.output.BinlogResultRow;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.GTidBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.BaseRowEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.ReadTimeoutExp;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.TableAlreadyModifyExp;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;

public class DefaultChannelStream implements ChannelStream {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultChannelStream.class);
    private static final BinlogPositionStoreTrigger NONE_STORE = new BinlogPositionStoreTrigger() {

        @Override
        public void triggerStoreBinlogPos() {
            // do nothing
        }

    };

    private boolean isSkipCurrentTrans = false;

    private ChannelBuffer channelBuffer = new DefaultChannelBuffer();
    private String channelId = UUID.randomUUID().toString();
    private BinlogPosition configBinlogPos;
    private TransactionRecognizer transactionRecognizer = new GTIDTransactionRecognizer();
    private BinlogPositionStore binlogPositionStore;
    private StreamSource streamSource;
    private long faultTolerantTimeout = 5000;
    private Consumer consumer;

    private final ChannelStreamContext context = new ChannelStreamContext();

    public ChannelBuffer getChannelBuffer() {
        return channelBuffer;
    }

    public void setChannelBuffer(ChannelBuffer channelBuffer) {
        this.channelBuffer = channelBuffer;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public BinlogPosition getConfigBinlogPos() {
        return configBinlogPos;
    }

    public void setConfigBinlogPos(BinlogPosition configBinlogPos) {
        this.configBinlogPos = configBinlogPos;
    }

    public TransactionRecognizer getTransactionRecognizer() {
        return transactionRecognizer;
    }

    public void setTransactionRecognizer(TransactionRecognizer transactionRecognizer) {
        this.transactionRecognizer = transactionRecognizer;
    }

    public BinlogPositionStore getBinlogPositionStore() {
        return binlogPositionStore;
    }

    public void setBinlogPositionStore(BinlogPositionStore binlogPositionStore) {
        this.binlogPositionStore = binlogPositionStore;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    public StreamSource getStreamSource() {
        return streamSource;
    }

    public void setStreamSource(StreamSource streamSource) {
        this.streamSource = streamSource;
    }

    public long getFaultTolerantTimeout() {
        return faultTolerantTimeout;
    }

    public void setFaultTolerantTimeout(long faultTolerantTimeout) {
        this.faultTolerantTimeout = faultTolerantTimeout;
    }

    @PostConstruct
    @Override
    public void start() {
        final CountDownLatch startProvide = new CountDownLatch(1);
        final CountDownLatch startConsumer = new CountDownLatch(1);
        createProviderThread(startProvide);
        createConsumerThread(startConsumer);
        try {
            startProvide.await(30000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        try {
            startConsumer.await(30000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private void createConsumerThread(final CountDownLatch startEvent) {
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                startEvent.countDown();
                consumerCore();
            }

        });
        t.setName("Consumer-" + channelId + t.getId());
        t.start();
    }

    private void consumerCore() {
        while (true) {
            // for shutdown
            if (context.shutDownTrigger) {
                context.shutDownConsumerEvent.countDown();
                break;
            }
            BufferableBinlogDataSet buffedDs = channelBuffer.pop(1000, TimeUnit.MILLISECONDS);
            if (buffedDs == null) {
                continue;
            }
            consumer.consumer(buffedDs.getBinlogDataSet(), createBinlogPositionStoreTrigger(buffedDs));
        }
    }

    private boolean isGtId() {
        return (configBinlogPos instanceof GTidBinlogPosition);
    }

    private BinlogPositionStoreTrigger createBinlogPositionStoreTrigger(BufferableBinlogDataSet buffedDs) {
        if (!(buffedDs instanceof PersistPosBufferableBinlogDataSet)) {
            return NONE_STORE;
        }

        final PersistPosBufferableBinlogDataSet ds = (PersistPosBufferableBinlogDataSet) buffedDs;
        return new BinlogPositionStoreTrigger() {

            @Override
            public void triggerStoreBinlogPos() {

                DefaultChannelStream.this.binlogPositionStore.store(ds.getPos(), channelId);
            }

        };
    }

    private void createProviderThread(final CountDownLatch startEvent) {
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                startEvent.countDown();
                providerThreadCore();
            }

        });
        t.setName("Provider-" + channelId + t.getId());
        t.start();
    }

    private void providerThreadCore() {
        while (true) {
            if (context.shutDownTrigger) {
                context.shutDownProviderEvent.countDown();
                break;
            }

            if (!ensureOpenStream()) {
                continue;
            }
            ValidBinlogOutput validOutput = null;
            try {
                validOutput = streamSource.readValidInfo();
            } catch (ReadTimeoutExp e) {
                LOG.info("channelId is {},read timeout,maybe meet network error.try to reopen.", this.channelId);
                this.streamSource.release();
                continue;

            } catch (TableAlreadyModifyExp e) {
                LOG.info("table has been modified.", e);
                continue;
            } catch (RuntimeException e) {
                LOG.info("channelId is " + channelId + ",meet unknown error.", e);
                this.streamSource.release();
                continue;
            }
            if (validOutput == null) {
                continue;
            }
            if (transactionRecognizer.isStart(validOutput)) {
                LOG.info("{},start trans {}", this.channelId, transactionRecognizer.getCurrentTransBeginPos());
            }
            if (transactionRecognizer.tryRecognizePos(validOutput)) {
                LOG.info("{},recognize pos {}", this.channelId, transactionRecognizer.getCurrentTransBeginPos());
            }
            if (validOutput.isRowEvent()) {
                if (isSkipCurrentTrans) {
                    LOG.info("{},skip row event of {}", this.channelId,
                            transactionRecognizer.getCurrentTransBeginPos());
                    continue;
                }
                
                LOG.info("{},dispatch row event of {}", this.channelId,
                        transactionRecognizer.getCurrentTransBeginPos());
                BufferableBinlogDataSet bufferDs = convert(validOutput);
                ensureDispatch(bufferDs);
                continue;
            }
            if (transactionRecognizer.isEnd(validOutput)) {
                context.setNextPos(transactionRecognizer.getCurrentTransBeginPos());
                BufferableBinlogDataSet bufferDs =
                        createPersistPosBufferableBinlogDataSet(transactionRecognizer.getCurrentTransBeginPos());
                ensureDispatch(bufferDs);
                if (isSkipCurrentTrans) {
                    isSkipCurrentTrans = false;
                }
                LOG.debug("{},end trans, {}", this.channelId, transactionRecognizer.getCurrentTransBeginPos());
            }
        }
    }

    private void ensureDispatch(BufferableBinlogDataSet bufferDs) {
        while (true) {
            // for shutdown
            if (context.shutDownTrigger) {
                break;
            }
            if (dispatchRowData(bufferDs)) {
                break;
            }
        }
    }

    private boolean dispatchRowData(BufferableBinlogDataSet ds) {
        return channelBuffer.push(ds, 1000, TimeUnit.MILLISECONDS);
    }

    private PersistPosBufferableBinlogDataSet createPersistPosBufferableBinlogDataSet(
            final BinlogPosition currentTransPos) {
        BinlogDataSet ds = BinlogDataSet.createPositionStoreTrigger(this.channelId, streamSource.getHostUrl(),
                transactionRecognizer.getGTId());
        return new PersistPosBufferableBinlogDataSet(ds, currentTransPos);
    }

    private BufferableBinlogDataSet convert(ValidBinlogOutput validOutput) {
        BinlogDataSet ds =
                new BinlogDataSet(this.channelId, streamSource.getHostUrl(), transactionRecognizer.getGTId());

        BaseRowEvent event = validOutput.getRowEvent();
        String tableName = event.getFullTableName();

        event.getColumnDefinitionList();
        if (!ds.getColumnDefMap().containsKey(tableName)) {
            List<ColumnDefinition> columnList = new ArrayList<>(event.getColumnDefinitionList().size());
            columnList.addAll(event.getColumnDefinitionList());
            ds.getColumnDefMap().put(tableName, columnList);
        }

        if (ds.getRowDataMap().containsKey(tableName)) {
            ds.getRowDataMap().get(tableName).addAll(event.getRowList());
        } else {
            List<BinlogResultRow> rowList = new LinkedList<>();
            rowList.addAll(event.getRowList());
            ds.getRowDataMap().put(tableName, rowList);
        }
        return new DefaultBufferableBinlogDataSet(ds);
    }

    private boolean ensureOpenStream() {
        if (this.streamSource.isOpen()) {
            return true;
        }
        try {
            streamSource.openStream(loadBinlogPosistion());
            return true;
        } catch (RuntimeException e) {
            LOG.error("ensureOpenStream error.", e);
            safeSleep(this.faultTolerantTimeout);
            return false;
        }

    }

    private void safeSleep(long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private BinlogPosition loadBinlogPosistion() {
        boolean isSupportGtId = isGtId();
        isSkipCurrentTrans = isSupportGtId;
        if (context.getNextPos() != null) {
            LOG.info("load binlog position {} from mem,channelId is {}.", context.getNextPos().toString(),
                    this.channelId);
            return context.getNextPos();
        }
        BinlogPosition loadPos = this.binlogPositionStore.load(channelId);
        if (loadPos != null) {
            if (!isSupportGtId && (loadPos instanceof GTidBinlogPosition)) {
                throw new RuntimeException("stored binlogPosition is not matched.");
            }
            LOG.info("load binlog position {} from store,channelId is {}.", loadPos.toString(), this.channelId);
            return loadPos;
        }
        if (this.configBinlogPos != null) {
            LOG.info("load binlog position {} from configure,channelId is {}.", configBinlogPos.toString(),
                    this.channelId);
            return configBinlogPos;
        }
        throw new RuntimeException("can not find binlog position.");
    }

    @PreDestroy
    @Override
    public void release() {
        context.shutDownTrigger = true;
        try {
            context.shutDownProviderEvent.await(30000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        try {
            context.shutDownConsumerEvent.await(30000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
    }

}
