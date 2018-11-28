package com.hiriver.streamsource.impl;

import com.hiriver.streamsource.DbHostInfo;
import com.hiriver.streamsource.DbHostInfoSupplier;
import com.hiriver.streamsource.StreamSource;
import com.hiriver.unbiz.mysql.lib.*;
import com.hiriver.unbiz.mysql.lib.filter.TableFilter;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogFileBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TimestampBinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.ReadTimeoutExp;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.text.ColumnValue;
import com.hiriver.unbiz.mysql.lib.protocol.text.ResultsetRowResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryResponse;
import com.hiriverunbiz.mysql.lib.exp.NotExpectPayloadException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 * 可在mysql uuid变化后(发生ha切换)重新定位新位点的StreamSource，可用来支持多种ha场景，
 * 比如vip、proxy(需要额外解析实际mysql server地址，通过DbHostInfoSupplier提供)等；
 *
 * 通过uuid进行判断是否发生ha切换，如发生切换则需要 1、把时间戳向前拨5分钟，2、通过二分查找定位前拨后的时间戳；
 *
 *
 * Q&A
 * 哪些场景需要重新做二分查找定位位点
 *   1、只配置了configPos，positionStore中无位点
 *   2、positionStore中位点的uuid信息同当前数据库uuid不一致时
 * 使用时需要注意哪些点
 *   1、重新定位位点后会有5分钟重复的binlog，需要注意消费幂等；
 *   2、二分查找时可能会用到较老的binlog文件，要保持binlog文件正确运维，否则会定位失败，不要随意删除磁盘binlog文件（开发、测试环境比较常见此问题，使用purge master logs to 'mysql-bin.000009'命令安全删除binlog文件）；
 *   3、重新定位位点可能会对数据库实例有网络流量耗损(一般来说不会很大)
 *
 * </pre>
 * <p>
 * created by Yang Huawei (xander.yhw@alibaba-inc.com) on 2018/8/29 22:44
 */
public class TimestampBasedStreamSource implements StreamSource, DbHostInfo {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimestampBasedStreamSource.class);
    private static final int VALID_BINLOG_START_OFFSET = 4;
    /**
     * 通信属性
     */
    private TransportConfig transportConfig = new TransportConfig();

    /**
     * 可用数据源supplier，返回的DbHostInfo最好稳定非随机以减少重新定位位点的过程
     */
    private DbHostInfoSupplier dbHostInfoSupplier;

    /**
     * 从库id，hiriver是从库
     */
    private int serverId;
    private int maxMaxPacketSize = 0;

    /**
     * 表过滤规则
     */
    private TableFilter tableFilter;

    /**
     * 重新定位位点时，前拨的时间，单位秒，一般来说不需要改
     */
    private int advanceSecondsToAvoidMistake = (int) TimeUnit.MINUTES.toSeconds(5);

    private volatile DbHostAndTransport currentDb;
    private volatile boolean open = false;


    @Override
    public void openStream(BinlogPosition binlogPos) {
        assertState(binlogPos instanceof TimestampBinlogPosition,
                "binlogPos类型非TimestampBinlogPosition，" + binlogPos);
        assertState(!open, "already opened");

        DbHostInfo dbHostInfo = this.dbHostInfoSupplier.available();
        assertState(dbHostInfo != null, "no available dbHostInfo");

        release();// release exist if need

        DbTarget dbTarget = new DbTarget(dbHostInfo);

        this.currentDb = new DbHostAndTransport(dbHostInfo, initTransportByDbHostInfo(dbTarget));

        binlogPos = findRealPosition((TimestampBinlogPosition) binlogPos, dbTarget,
                this.currentDb.serverUuid, advanceSecondsToAvoidMistake);// 可能连接信息发生变化的情况，重新定位实际位点

        currentDb.transport.dump(binlogPos);
        open = true;
    }


    @Override
    public ValidBinlogOutput readValidInfo() throws ReadTimeoutExp {
        ValidBinlogOutput result = this.currentDb.transport.getBinlogOutputImmediately();
        if (result != null) {
            result.setServerUuid(this.currentDb.serverUuid);
        }
        return result;
    }

    /**
     * <pre>
     * 定位位点
     *  1、如果入参targetPosition.uuid同目标serverUuid一致的话，则直接返回入参targetPosition
     *  2、否则的话，向前调整时间戳advanceSecondsWhenToAvoidMistake，进行按照时间戳定位位点。
     * </pre>
     *
     * @param paramPosition                    至少要有timestamp
     * @param advanceSecondsWhenToAvoidMistake binlog中的事件顺序和事务commit
     *                                         event的timestamp顺序近似相似，但仍有风险，所有提前一段时间，确保无误
     */
    private static TimestampBinlogPosition findRealPosition(TimestampBinlogPosition paramPosition,
            final DbTarget dbTarget, String serverUuid, long advanceSecondsWhenToAvoidMistake) {

        assertState(paramPosition.getTimestamp() <= System.currentTimeMillis() / 1000, "时间位点不可晚于当前时间");
        assertState(advanceSecondsWhenToAvoidMistake > 0, "前拨秒数不可设置为0");

        // check uuid
        if (StringUtils.equalsIgnoreCase(paramPosition.getServerUuid(), serverUuid)) {
            return paramPosition;
        }
        long targetTimestamp = paramPosition.getTimestamp() - advanceSecondsWhenToAvoidMistake;

        LOGGER.info("target timestamp position to find:{}", targetTimestamp);

        final TextProtocolBlockingTransport textProtocolBlockingTransport =
                new TextProtocolBlockingTransportImpl(dbTarget.getHost(), dbTarget.getPort(),
                        dbTarget.getUserName(), dbTarget.getPassword());

        try {
            textProtocolBlockingTransport.open();

            BinlogFileBinlogPosition end = getEndBinlogFilePos(textProtocolBlockingTransport);
            LOGGER.info("current binlog position end:{}", end);

            List<String> binaryLogFiles = getAllBinaryLogNames(textProtocolBlockingTransport);


            String firstFileLessThanTarget = findLastFileStartLeTimestamp(targetTimestamp, binaryLogFiles,
                    new ExtractValidBeginTimestampFunction() {
                        @Override
                        public long extract(String binlogFileName) {
                            return getTimestampOfFirstEvent(dbTarget, textProtocolBlockingTransport,
                                    binlogFileName);
                        }
                    });
            if (firstFileLessThanTarget == null) {// 抛出异常而不使用第一个binlog文件，防止丢事件
                throw new IllegalArgumentException("目标时间戳早于binlog中最早的事件，可能会导致消费binlog事件丢失");
            }

            TimestampBinlogPosition resultPosition = findPositionLeTargetTimestampInOneBinlogFile(dbTarget,
                    serverUuid, targetTimestamp, textProtocolBlockingTransport, end, firstFileLessThanTarget);

            LOGGER.info("findRealPosition resultPosition:{} for targetTimestamp:{}", resultPosition, targetTimestamp);
            return resultPosition;
        } finally {
            try {
                textProtocolBlockingTransport.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static TimestampBinlogPosition findPositionLeTargetTimestampInOneBinlogFile(
            DbTarget dbTarget, String serverUuid, long targetTimestamp,
            TextProtocolBlockingTransport textProtocolBlockingTransport, BinlogFileBinlogPosition end,
            String firstFileLessThanTarget) {

        BinlogStreamBlockingTransportImpl binlogStream = new BinlogStreamBlockingTransportImpl(
                dbTarget.getHost(), dbTarget.getPort(), dbTarget.getUserName(), dbTarget.getPassword());
        binlogStream.setTableFilter(new TableFilter() {// avoid permission problem
            @Override
            public boolean filter(String dbName, String tableName) {
                return false;
            }
        });
        try {
            binlogStream
                    .dump(new BinlogFileBinlogPosition(firstFileLessThanTarget, VALID_BINLOG_START_OFFSET));

            Long resultTimestamp = null;
            String resultBinlogFile = null;
            Long resultPos = null;

            while (true) {
                BinlogEvent binlogEvent = binlogStream.readWithoutSpecialEvent();

                if (binlogEvent == null || binlogEvent.getOccurTime() <= 0
                        || binlogEvent.getBinlogEventPos() <= 0) {
                    // fake event, ROTATE_EVENT 、 HEARTBEAT_LOG_EVENT
                    continue;
                }
                long timestamp = binlogEvent.getOccurTime();

                if (resultTimestamp == null || timestamp <= targetTimestamp) {
                    // first event or
                    resultTimestamp = timestamp;
                    resultBinlogFile = binlogStream.currentBinlogFile();
                    resultPos = binlogEvent.getBinlogEventPos();
                } else {
                    break;
                }
                if (le(end.getBinlogFileName(), end.getPos(), resultBinlogFile, resultPos)) {
                    // 之前找到的最后一个点小于等于result，防止后续阻塞等待新binlog
                    LOGGER.info("result pos {}:{} behind/equals end position:{}", resultBinlogFile, resultPos,
                            end);
                    break;
                }
            }
            return new TimestampBinlogPosition(resultTimestamp, serverUuid, resultBinlogFile, resultPos);
        } finally {
            closeBinlogDump(textProtocolBlockingTransport, binlogStream);
        }
    }

    /**
     * <pre>
     * 按照时间戳定位第一个
     *  1、目标时间戳较近的话按照binlog文件列表逆序查找一两个文件
     *  2、否则按照按照文件列表二分查找
     * </pre>
     * <p>
     * visible for test
     *
     * @param targetTimestamp        目标时间戳
     * @param binlogFiles            binlog文件有序列表
     * @param beginTimestampFunction 提取binlog文件中的第一个有效事件的timestamp
     * @return null表示未找到
     */
    static String findLastFileStartLeTimestamp(long targetTimestamp, List<String> binlogFiles,
            ExtractValidBeginTimestampFunction beginTimestampFunction) {

        int max = binlogFiles.size() - 1;

        // 先尝试最后一个文件，最后一个文件的第一个事件时间le目标时间，则返回最后一个文件
        if (beginTimestampFunction.extract(binlogFiles.get(max)) <= targetTimestamp) {
            return binlogFiles.get(max);
        }
        // 近几个小时内，逆序再尝试一次
        if (max > 0 && targetTimestamp + TimeUnit.HOURS.toSeconds(3) >= System.currentTimeMillis()) {
            max--;
            if (beginTimestampFunction.extract(binlogFiles.get(max)) <= targetTimestamp) {
                return binlogFiles.get(max);
            }
        }
        int min = 0;
        // check 第一个文件，max == 0判断表示已经获取过第一个文件中的开始timestamp且gt目标timestamp
        if (max == 0 || !(beginTimestampFunction.extract(binlogFiles.get(min)) <= targetTimestamp)) {
            return null;// 未找到
        }

        int mid;
        while ((mid = (min + max) / 2) > min) {
            if (beginTimestampFunction.extract(binlogFiles.get(mid)) <= targetTimestamp) {
                min = mid;
            } else {
                max = mid;
            }
        }
        return binlogFiles.get(mid);

    }


    private BinlogStreamBlockingTransportImpl initTransportByDbHostInfo(DbTarget dbTarget) {
        BinlogStreamBlockingTransportImpl binlogStreamBlockingTransport =
                new BinlogStreamBlockingTransportImpl(dbTarget.getHost(), dbTarget.getPort(),
                        dbTarget.getUserName(), dbTarget.getPassword());
        binlogStreamBlockingTransport.setServerId(this.getServerId());
        binlogStreamBlockingTransport.setTransportConfig(this.getTransportConfig());
        binlogStreamBlockingTransport.setTableFilter(this.getTableFilter());
        binlogStreamBlockingTransport.setMaxMaxPacketSize(this.getMaxMaxPacketSize());
        return binlogStreamBlockingTransport;
    }

    private static BinlogFileBinlogPosition getEndBinlogFilePos(
            TextProtocolBlockingTransport textProtocolBlockingTransport) {
        String lastFileName = null;
        Long lastPos = null;
        TextCommandQueryResponse response = textProtocolBlockingTransport.execute("show master status");

        for (ColumnValue columnValue : response.getRowList().get(0).getValueList()) {
            if (StringUtils.equalsIgnoreCase("File", columnValue.getColumnName())) {
                lastFileName = columnValue.getValueAsString();
            } else if (StringUtils.equalsIgnoreCase("Position", columnValue.getColumnName())) {
                lastPos = columnValue.getValueAsLong();
            }
        }
        assertState(lastFileName != null, "null lastFileName");
        assertState(lastPos != null, "null lastPos");
        return new BinlogFileBinlogPosition(lastFileName, lastPos);
    }


    /**
     * 当前数据库中所有binlog文件名列表
     */
    private static List<String> getAllBinaryLogNames(
            TextProtocolBlockingTransport textProtocolBlockingTransport) {
        TextCommandQueryResponse response;
        List<String> binaryLogFiles = new ArrayList<>();
        response = textProtocolBlockingTransport.execute("show binary logs");
        for (ResultsetRowResponse resultsetRowResponse : response.getRowList()) {
            for (ColumnValue columnValue : resultsetRowResponse.getValueList()) {
                if (StringUtils.equals("Log_name", columnValue.getColumnName())) {
                    binaryLogFiles.add(columnValue.getValueAsString());
                }
            }
        }
        assertState(binaryLogFiles.size() > 0, "empty binaryLogFiles");
        return binaryLogFiles;
    }

    interface ExtractValidBeginTimestampFunction {
        long extract(String binlogFileName);
    }

    /**
     * 返回binlog文件中第一个有效事件的时间戳
     */
    private static long getTimestampOfFirstEvent(DbTarget dbTarget,
            TextProtocolBlockingTransport textProtocolBlockingTransport, String binlogFile) {
        BinlogStreamBlockingTransportImpl binlogStream = new BinlogStreamBlockingTransportImpl(
                dbTarget.getHost(), dbTarget.getPort(), dbTarget.getUserName(), dbTarget.getPassword());
        binlogStream.setTableFilter(new TableFilter() {// avoid permission problem
            @Override
            public boolean filter(String dbName, String tableName) {
                return false;
            }
        });
        try {
            binlogStream.dump(new BinlogFileBinlogPosition(binlogFile, VALID_BINLOG_START_OFFSET));
            while (true) {
                BinlogEvent binlogEvent = binlogStream.readWithoutSpecialEvent();
                if (binlogEvent == null || binlogEvent.getOccurTime() <= 0) {
                    // 需要过滤一些fake event，occurTime可能为0
                    continue;
                }
                LOGGER.info("binlogFile:{}, first event timestamp:{}", binlogFile,
                        binlogEvent.getOccurTime());
                return binlogEvent.getOccurTime();
            }
        } finally {
            closeBinlogDump(textProtocolBlockingTransport, binlogStream);
        }
    }


    /**
     * 尽量关闭binlogStream，防止占用mysql总连接数
     */
    private static void closeBinlogDump(TextProtocolBlockingTransport textProtocolBlockingTransport,
            BinlogStreamBlockingTransportImpl binlogStream) {
        Long connectionId = binlogStream.mysqlConnectionId();
        binlogStream.close();
        if (connectionId != null) {
            try {
                textProtocolBlockingTransport.execute("KILL CONNECTION " + connectionId);
                //mysql thread_id是一个递增的值，不存在误杀其他mysql线程的可能
            } catch (NotExpectPayloadException ignored) {
            }
        }
    }

    private static void assertState(boolean state, String message) {
        if (!state) {
            throw new IllegalStateException(message);
        }
    }

    /**
     * 根据文件名称、偏移量，比较第一个位点是否小于等于第二个位点
     */
    private static boolean le(String firstFile, long firstPos, String secondFile, long secondPos) {
        int firstFileIndex = Integer.parseInt(StringUtils.substringAfterLast(firstFile, "."));
        int secondFileIndex = Integer.parseInt(StringUtils.substringAfterLast(secondFile, "."));

        if (firstFileIndex < secondFileIndex) {
            return true;
        } else if (firstFileIndex > secondFileIndex) {
            return false;
        } else {
            return firstPos <= secondPos;
        }
    }


    private static class DbTarget implements DbHostInfo {
        private String host;
        private int port;
        private String userName;
        private String password;

        public DbTarget(DbHostInfo dbHostInfo) {
            String[] array = dbHostInfo.getHostUrl().split(":");
            this.port = Integer.parseInt(array[1]);
            this.host = array[0];
            this.userName = dbHostInfo.getUserName();
            this.password = dbHostInfo.getPassword();
        }

        @Override
        public String getHostUrl() {
            return this.host + ":" + this.port;
        }

        @Override
        public String getUserName() {
            return this.userName;
        }

        @Override
        public String getPassword() {
            return this.password;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }

    private class DbHostAndTransport implements Closeable {
        private final DbHostInfo dbHostInfo;
        private final String serverUuid;
        private final BinlogStreamBlockingTransport transport;

        DbHostAndTransport(DbHostInfo dbHostInfo, BinlogStreamBlockingTransport transport) {
            this.dbHostInfo = dbHostInfo;
            this.transport = transport;
            this.serverUuid = fetchUuid();
        }


        private String fetchUuid() {
            String[] array = dbHostInfo.getHostUrl().split(":");
            int port = Integer.parseInt(array[1]);
            String host = array[0];
            TextProtocolBlockingTransport textProtocolBlockingTransport =
                    new TextProtocolBlockingTransportImpl(host, port, dbHostInfo.getUserName(),
                            dbHostInfo.getPassword());
            try {
                textProtocolBlockingTransport.open();
                TextCommandQueryResponse response =
                        textProtocolBlockingTransport.execute("show variables like \"server_uuid\"");
                for (ColumnValue columnValue : response.getRowList().get(0).getValueList()) {
                    if (StringUtils.equals("Value", columnValue.getColumnName())) {
                        return columnValue.getValueAsString();
                    }
                }
            } finally {
                try {
                    textProtocolBlockingTransport.close();
                } catch (Exception ignored) {
                }
            }
            throw new IllegalStateException("fetch uuid fail");
        }

        @Override
        public void close() {
            if (this.transport != null) {
                try {
                    this.transport.close();
                } catch (Exception ignored) {
                }
            }
        }
    }


    @Override
    public String getHostUrl() {
        if (this.currentDb == null) {
            return null;
        }
        return this.currentDb.dbHostInfo.getHostUrl();
    }

    @Override
    public String getUserName() {
        if (this.currentDb == null) {
            return null;
        }
        return this.currentDb.dbHostInfo.getUserName();
    }

    @Override
    public String getPassword() {
        if (this.currentDb == null) {
            return null;
        }
        return this.currentDb.dbHostInfo.getPassword();
    }


    @Override
    public void release() {
        if (currentDb != null) {
            this.currentDb.close();
            currentDb = null;
        }
        open = false;
    }

    @Override
    public boolean isOpen() {
        /**
         * 最近是否出现异常 & check是否发生主库漂移
         */
        return open;
    }


    public TransportConfig getTransportConfig() {
        return transportConfig;
    }

    public void setTransportConfig(TransportConfig transportConfig) {
        this.transportConfig = transportConfig;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public int getMaxMaxPacketSize() {
        return maxMaxPacketSize;
    }

    public void setMaxMaxPacketSize(int maxMaxPacketSize) {
        this.maxMaxPacketSize = maxMaxPacketSize;
    }

    public TableFilter getTableFilter() {
        return tableFilter;
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }


    public DbHostInfoSupplier getDbHostInfoSupplier() {
        return dbHostInfoSupplier;
    }

    public void setDbHostInfoSupplier(DbHostInfoSupplier dbHostInfoSupplier) {
        this.dbHostInfoSupplier = dbHostInfoSupplier;
    }

    public int getAdvanceSecondsToAvoidMistake() {
        return advanceSecondsToAvoidMistake;
    }

    public void setAdvanceSecondsToAvoidMistake(int advanceSecondsToAvoidMistake) {
        this.advanceSecondsToAvoidMistake = advanceSecondsToAvoidMistake;
    }
}

