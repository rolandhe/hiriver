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

/**
 * 缺省的{@link ChannelStream}，内部会启动两个线程，一个用于从mysql数据源读取binlog数据， 另一个用于消费数据。在GTID模式下，在与mysql断开连接后重连时它会自动跳过第一个事务，这样防止
 * 该事务数据被多次重发(配置除外)。而在binlog file name + pos模式下，它不会跳过事务。<br>
 * <ul>
 * <li>在gtid模式下，配置第一个需要的事务</li>
 * <li>binlog file name + pos,一定要配置事务的开始或者上一个事务的结束点，否则容易丢失TableMamEvent，造成后续的 Row event不同获取表元数据，抛出异常</li>
 * </ul>
 * 
 * @author hexiufeng
 *
 */
public class DefaultChannelStream implements ChannelStream {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultChannelStream.class);

    /**
     * 不存储同步点实现
     */
    private static final BinlogPositionStoreTrigger NONE_STORE = new BinlogPositionStoreTrigger() {

        @Override
        public void triggerStoreBinlogPos() {
            // do nothing
        }

    };

    /**
     * 描述是否要跳过当前事务数据，用于GTID模式
     */
    private boolean isSkipCurrentTrans = false;

    /**
     * 缺省ChannelBuffer
     */
    private ChannelBuffer channelBuffer = new DefaultChannelBuffer();
    /**
     * 当前数据流的名字，用于区分不同的数据流，应用方最后自行配置，缺省是uuid
     */
    private String channelId = UUID.randomUUID().toString();
    /**
     * 应用方配置的同步点位置
     */
    private BinlogPosition configBinlogPos;
    /**
     * 事务的识别器，缺省是GTIDTransactionRecognizer
     */
    private TransactionRecognizer transactionRecognizer = new GTIDTransactionRecognizer();
    /**
     * 同步点存储器
     */
    private BinlogPositionStore binlogPositionStore;
    /**
     * 连接到mysql的数据流，可能是{@link com.hiriver.streamsource.impl.MysqlStreamSource}数据流， 也可能是
     * {@link com.hiriver.streamsource.impl.MysqlStreamSource.HAStreamSource}数据流。如果是 mysql
     * 5.6.9+版本，强烈建议使用GTID模式，并且使用HAStreamSource，系统能够自动的切换数据源，实现HA
     */
    private StreamSource streamSource;

    /**
     * 当与mysql失去连接后，线程sleep的时间，超过该时间后再进行重连
     */
    private long faultTolerantTimeout = 5000;
    /**
     * 指定的consumer实现，用于消费数据
     */
    private Consumer consumer;

    /**
     * 当前数据流的上下文
     */
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

    /**
     * 开启mysql binlog数据接收线程和数据消费线程
     */
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

    /**
     * 创建接收mysql binlog数据流的线程
     * 
     * @param startEvent 等待线程是否开启成功的事件
     */
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

    /**
     * 接收mysql binlog数据流的核心处理逻辑。<br>
     * <ul>
     * <li>保证与mysql的数据流是打开的</li>
     * <li>读取事件数据</li>
     * <li>控制事务</li>
     * <li>发送数据到{@link ChannelBuffer}</li>
     * 
     * </ul>
     */
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

    /**
     * 创建消费者线程
     * 
     * @param startEvent 线程是否启动成功的监控事件
     */
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

    /**
     * 消费者线程核心业务处理。<br>
     * <u>
     * <li>从 {@link ChannelBuffer}读取数据</li>
     * <li>调用{@link Consumer#consumer(BinlogDataSet, BinlogPositionStoreTrigger)} 消费数据</li>
     * </ul>
     */
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

    /**
     * 根据初始同步点配置信息判断当前是否是GTID模式
     * 
     * @return 是否是GTID模式
     */
    private boolean isGtId() {
        return (configBinlogPos instanceof GTidBinlogPosition);
    }

    /**
     * 创建{@link Consumer#consumer(BinlogDataSet, BinlogPositionStoreTrigger)}需要的{@link BinlogPositionStoreTrigger}回调。
     * 用于消费者触发记录同步点。<br>
     * <p>如果当前buffedDs是否具体的数据，则不需要记录同步点，如果是事务的结束，则需要 </p>
     * 
     * 
     * @param buffedDs 需要消费的数据
     * @return  同步点记录回调
     */
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

    /**
     * 分发数据
     * 
     * @param bufferDs row event数据或者记录同步点信号
     */
    private void ensureDispatch(BufferableBinlogDataSet bufferDs) {
        while (true) {
            // for shutdown
            if (context.shutDownTrigger) {
                break;
            }
            if (channelBuffer.push(bufferDs, 1000, TimeUnit.MILLISECONDS)) {
                break;
            }
        }
    }

    /**
     * 当事务结束时，创建记录同步点信号的数据 {@link PersistPosBufferableBinlogDataSet}
     * 
     * @param currentTransPos 当前事务的同步点
     * @return {@link PersistPosBufferableBinlogDataSet}对象
     */
    private PersistPosBufferableBinlogDataSet createPersistPosBufferableBinlogDataSet(
            final BinlogPosition currentTransPos) {
        BinlogDataSet ds = BinlogDataSet.createPositionStoreTrigger(this.channelId, streamSource.getHostUrl(),
                transactionRecognizer.getGTId());
        return new PersistPosBufferableBinlogDataSet(ds, currentTransPos);
    }

    /**
     * 把来自binlog解析的有效的{@link ValidBinlogOutput}转换成可以缓存分发的{@link BufferableBinlogDataSet}对象
     * 
     * @param validOutput {@link ValidBinlogOutput}对象
     * @return {@link BufferableBinlogDataSet}对象
     */
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

    /**
     * 确保打开与mysql的数据流连接
     * 
     * @return 是否打开成功
     */
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

    /**
     * 线程休眠指定的时间
     * 
     * @param timeout 指定的时间
     */
    private void safeSleep(long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    /**
     * 加载有效的同步点<br>
     * <ul>
     * <li>先从内存中读取 </li>
     * <li>在从{@link BinlogPositionStore}读取 </li>
     * <li>读取配置</li>
     * </ul>
     * 
     * @return
     */
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
            isSkipCurrentTrans = false;
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
