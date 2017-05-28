package com.hiriver.unbiz.mysql.lib;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiriver.unbiz.mysql.lib.filter.TableFilter;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.ERRPacket;
import com.hiriver.unbiz.mysql.lib.protocol.OKPacket;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.Request;
import com.hiriver.unbiz.mysql.lib.protocol.Response;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogContext;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.BinlogEventHeader;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.RegisterRequest;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TableMeta;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.TableMetaProvider;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidBinlogOutput;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.ValidEventType;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.BaseRowEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.EventFactory;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.FormatDescriptionEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.GTidEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.QueryEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.RotateEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.TableMapEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.event.XidEvent;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.exp.ReadTimeoutExp;
import com.hiriver.unbiz.mysql.lib.protocol.binlog.extra.BinlogPosition;
import com.hiriver.unbiz.mysql.lib.protocol.text.ColumnDefinitionResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.FieldListCommandResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.ResultsetRowResponse;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandFieldListRequest;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryRequest;
import com.hiriver.unbiz.mysql.lib.protocol.text.TextCommandQueryResponse;
import com.hiriver.unbiz.mysql.lib.protocol.tool.PacketTool;
import com.hiriverunbiz.mysql.lib.exp.InvalidMysqlDataException;

/**
 * 用于binlog复制的堵塞模式的通信实现。一般用于mysql的主从同步实现。<br>
 * <p>
 * 在本实现中也实现一部分Text protocol，这是因为在5.6版本中同步需要先判断一些
 * server端的配置信息或者配置session参数，比如调用SET @master_binlog_checksum= @@global.binlog_checksum
 * 配置checksum参数，后续也增加自动识别gtid mode的实现，这些都需需要实现Text protocol
 * </p>
 * 
 * @author hexiufeng
 *
 */
public class BinlogStreamBlockingTransportImpl extends AbstractBlockingTransport
    implements BinlogStreamBlockingTransport {
  private static final Logger LOGGER = LoggerFactory.getLogger(BinlogStreamBlockingTransportImpl.class);
  private final BinlogContext context = new BinlogContext();
  private int serverId;
  private TableFilter tableFilter;
  private boolean checkSum = false;

  private Position defaultPos = Position.factory();
  /**
   * 在解析Response时需要持续读取数据的ResultContentReader接口默认实现
   */
  private final ResultContentReader resultContentReader = new ResultContentReader() {

    @Override
    public byte[] readNextPacketPayload() {
      return readResponsePayload();
    }

  };
  /**
   * 表元数据的提供者实现，从db中读取，基于 Text Protocol的COM_FIELD_LIST指令，比desc table name sql更有效， 返回更多数据，
   * 使用show columns可读取enum or set的元数据
   */
  private TableMetaProvider tableMetaProvider = new TableMetaProvider() {
    private final Map<String, TableMeta> cache = new HashMap<String, TableMeta>();

    @Override
    public TableMeta getTableMeta(long tableId, TableMapEvent tableMapEvent) {
      String schemaName = tableMapEvent.getSchema();
      String tableName = tableMapEvent.getTableName();
      String fullTableName = schemaName + "." + tableName;
      if (cache.containsKey(fullTableName)) {
        TableMeta tableMeta = cache.get(fullTableName);
        if (tableMeta.getTableId() == tableId) {
          return tableMeta;
        }
      }
      TableMeta tableMeta = new TableMeta(tableId);
      TextProtocolBlockingTransport textTrans = new TextProtocolBlockingTransportImpl(getHost(), getPort(),
          getUserName(), getPassword(), schemaName, getTransportConfig());

      textTrans.open();
      try {
        readFieldListMeta(tableName, tableMeta, textTrans);
        if(tableMapEvent.hasEnumOrSet()){
          Map<String,ColumnDefinition> map = new HashMap<>();
          for(ColumnDefinition definition : tableMeta.getColumnMetaList()){
            map.put(definition.getColumName(),definition);
          }
          readEnumOrSetMeta(tableName, map, textTrans);
        }

      } finally {
        textTrans.close();
      }
      cache.put(fullTableName, tableMeta);
      return tableMeta;
    }

    /**
     *
     * 读取表字段元数据
     *
     * @param tableName
     * @param tableMeta
     * @param textTrans
     */
    private void readFieldListMeta(String tableName, TableMeta tableMeta, TextProtocolBlockingTransport textTrans) {
      TextCommandFieldListRequest request = new TextCommandFieldListRequest(tableName);
      FieldListCommandResponse response = textTrans.showFieldList(request);
      for (ColumnDefinitionResponse coldef : response.getColumnList()) {
        ColumnDefinition def = createColumnDefinition(coldef);
        tableMeta.addColumn(def);
      }
    }

    /**
     * 读取enum 或者set类型的元数据，通过show columns sql命令读取类型，然后解析
     *
     * @param tableName
     * @param map
     * @param textTrans
     */
    private void readEnumOrSetMeta(String tableName, Map<String,ColumnDefinition> map, TextProtocolBlockingTransport textTrans) {
      String sql = "show columns from " + tableName + " where Type like 'set(%' or Type like 'enum(%'";
      TextCommandQueryResponse response = textTrans.execute(sql);
      if(response.getRowList() == null || response.getRowList().size() == 0){
        return;
      }
      for (ResultsetRowResponse row  : response.getRowList()) {
        String name = row.getValueList().get(0).getValueAsString();
        String value = row.getValueList().get(1).getValueAsString();
        LOGGER.info("enum or set of {}.{}:{}",tableName,name,value);
        if(value.startsWith("enum(")){
          String enumValue = value.substring("enum(".length(),value.length() - 1);
          map.get(name).getEnumList().addAll(parseEnumOrSetValue(enumValue));
        }
        if(value.startsWith("set(")){
          String setValue = value.substring("set(".length(),value.length() - 1);
          map.get(name).getSetList().addAll(parseEnumOrSetValue(setValue));
        }
      }
    }
    private List<String> parseEnumOrSetValue(String value){
      String[] array = value.split(",");
      List<String> list = new ArrayList<>(array.length);
      for(String v : array){
        list.add(v.substring(1,v.length() - 1));
      }
      return list;
    }


    /**
     * 从{@link ColumnDefinitionResponse}转换成外部可以识别的 {@link ColumnDefinition}
     * 
     * @param coldef ColumnDefinitionResponse定义
     * @return ColumnDefinition
     */
    private ColumnDefinition createColumnDefinition(ColumnDefinitionResponse coldef) {
      ColumnDefinition def = new ColumnDefinition();
      def.setColumName(coldef.getName());
      def.setCharset(coldef.getCharset());
      def.setKey(coldef.isKey());
      def.setPrimary(coldef.isPrimayKey());
      def.setType(coldef.getType());
      def.setUnique(coldef.isUniqueKey());
      def.setUnsigned(coldef.isUnsigned());
      def.setLen(coldef.getColumnLength());
      return def;
    }

  };

  public BinlogStreamBlockingTransportImpl() {

  }

  public BinlogStreamBlockingTransportImpl(String host, int port, String userName, String password) {
    super(host, port, userName, password, null);
  }

  @Override
  public boolean ping() {
    return false;
  }

  @Override
  protected Logger getSubClassLogger() {
    return LOGGER;
  }

  @Override
  protected void afterOpen(){
    autoDectectCheckSum();
    if (checkSum) {
      int affectRows = executeSQL("SET @master_binlog_checksum= @@global.binlog_checksum");
      getSubClassLogger().info("set checkSum, result is {}.", affectRows);
    }
  }

  @Override
  protected void intiTransport(String sql) {
    String upSql = sql.toUpperCase().replaceAll("\\s+", " ");
    // binlog 不需要事务
    if(upSql.startsWith("SET AUTOCOMMIT")){
      return;
    }
    executeSQL(sql);
  }

  /**
   * 从数据库中刷新是否开启了checksum
   */
  private void autoDectectCheckSum(){
    TextProtocolBlockingTransport textTrans = new TextProtocolBlockingTransportImpl(getHost(), getPort(),
            getUserName(), getPassword(), getTransportConfig());

    textTrans.open();
    try {
      TextCommandQueryResponse response = textTrans.execute("show VARIABLES like 'binlog_checksum'");
      if(response.getRowList().size() == 0){
        return;
      }
      String checkSumValue = response.getRowList().get(0).getValueList().get(1).getValueAsString();
      checkSum = !"NONE".equalsIgnoreCase(checkSumValue);
      LOGGER.info("read checksum {} from db {}:{}.",checkSumValue,getHost(),getPort());
    } finally {
      textTrans.close();
    }
  }

  /**
   * 基于Text Protocol实现执行sql的功能
   * 
   * @param sql sql
   * @return 本次执行影响的行数
   */
  private int executeSQL(String sql) {
    Response resp = executeSQLCore(sql);
    if (resp instanceof OKPacket) {
      return (int) ((OKPacket) resp).getAffectedRows();
    }
    return 0;
  }

  /**
   * 基于Text Protocol实现执行sql的功能,可以返回结果
   * 
   * @param sql sql
   * @return 返回结果
   */
  private Response executeSQLCore(String sql) {
    TextCommandQueryRequest query = new TextCommandQueryRequest(sql);
    super.writeRequest(query);
    byte[] buffer = super.readResponsePayload();
    checkErrPacket(buffer);
    if (PacketTool.isOkPackete(buffer)) {
      OKPacket okPacket = new OKPacket();
      okPacket.parse(buffer);
      return okPacket;
    }
    TextCommandQueryResponse queryResp = new TextCommandQueryResponse(resultContentReader);
    queryResp.parse(buffer);
    return queryResp;
  }

  @Override
  public void dump(BinlogPosition binlogPos) {
    context.setTableMetaProvider(tableMetaProvider);
    super.open();
    registerSlave();
    super.writeRequest(binlogPos.packetDumpRequest(this.serverId));
    readFormatEvent();
    super.readTimeoutHanlder = new SocketReadTimeoutHanlder() {

      @Override
      public void handle(String message, Exception e) {
        LOGGER.error(message, e);
        throw new ReadTimeoutExp();
      }

    };
  }

  /**
   * 在dump执行在主库注册当前从库
   */
  private void registerSlave() {
    // register
    Request reg = new RegisterRequest(serverId);
    super.writeRequest(reg);
    byte[] buf = super.readResponsePayload();

    if (!PacketTool.isOkPackete(buf)) {
      ERRPacket ep = new ERRPacket();
      ep.setCheckSum(this.checkSum);
      ep.parse(buf);
      throw new InvalidMysqlDataException(ep.getErrorMessage());
    }
  }

  @Override
  public ValidBinlogOutput getBinlogOutput() {
    Position pos = Position.factory();
    while (true) {
      BinlogEvent event = readEvent(pos);
      if (processSpecialEvent(event)) {
        continue;
      }
      ValidBinlogOutput ve = distinguishEvent(event);
      if (ve != null) {
        return ve;
      }
    }
  }

  @Override
  public ValidBinlogOutput getBinlogOutputImmediately() {

    BinlogEvent event = readEvent(defaultPos);
    if (processSpecialEvent(event)) {
      return null;
    }
    return distinguishEvent(event);
  }

  private boolean processSpecialEvent(final BinlogEvent event) {
    if (event instanceof RotateEvent) {
      context.setRotateEvent((RotateEvent) event);
      return true;
    }
    if (event instanceof TableMapEvent) {
      context.putCurrentTableMapEvent((TableMapEvent) event);
      return true;
    }
    return false;
  }

  /**
   * 识别读取到事件是否是有效事件
   * 
   * @param event 当前读取到binlog事件
   * @return 有效事件描述
   */
  private ValidBinlogOutput distinguishEvent(BinlogEvent event) {
    if (event instanceof GTidEvent) {
      return new ValidBinlogOutput(event, context.getRotateEvent().getNextBinlogName(), ValidEventType.GTID);
    }
    if (event instanceof BaseRowEvent) {
      return new ValidBinlogOutput(event, context.getRotateEvent().getNextBinlogName(), ValidEventType.ROW);
    }
    if (event instanceof XidEvent) {
      return new ValidBinlogOutput(event, context.getRotateEvent().getNextBinlogName(), ValidEventType.TRANS_COMMIT);
    }
    if (event instanceof QueryEvent) {
      QueryEvent qEvent = (QueryEvent) event;
      if ("BEGIN".equals(qEvent.getQuery())) {
        return new ValidBinlogOutput(event, context.getRotateEvent().getNextBinlogName(), ValidEventType.TRAN_BEGIN);
      }
      if ("ROLLBACK".equals(qEvent.getQuery())) {
        return new ValidBinlogOutput(event, context.getRotateEvent().getNextBinlogName(),
            ValidEventType.TRANS_ROLLBACK);
      }
      return null;
    }

    return null;
  }

  /**
   * 读取当session的FORMAT_DESCRIPTION_EVENT事件
   */
  private void readFormatEvent() {
    Position pos = Position.factory();
    while (true) {
      BinlogEvent event = readEvent(pos);
      if (event instanceof RotateEvent) {
        context.setRotateEvent((RotateEvent) event);
        continue;
      }
      if (event instanceof FormatDescriptionEvent) {
        context.setForamtDescEvent((FormatDescriptionEvent) event);
        break;
      }
    }
  }

  /**
   * 从binlog读取事件
   * 
   * @param pos 给定一个可重用的Pos，用于控制缓存读取的位置
   * @return binlog事件
   */
  private BinlogEvent readEvent(Position pos) {
    byte[] buf = super.readResponsePayload();

    if (!PacketTool.isOkPackete(buf)) {
      if (PacketTool.isErrPackete(buf)) {
        ERRPacket ep = new ERRPacket();
        ep.setCheckSum(this.checkSum);
        ep.parse(buf);
        throw new InvalidMysqlDataException(ep.getErrorMessage());
      }
      if (PacketTool.isEofPacket(buf)) {
        throw new InvalidMysqlDataException("eof");
      }
      throw new InvalidMysqlDataException("recieve invalid packet");
    }

    pos.reset();
    pos.forwardPos();
    BinlogEventHeader eventHeader = new BinlogEventHeader();

    eventHeader.parse(buf, pos);
    BinlogEvent event = EventFactory.factory(eventHeader.getEventType(), eventHeader.getLogPos(), context, checkSum);
    tryParseTableIdOfRowEvent(event, buf, pos);
    if (!filter(event)) {
      return null;
    }
    event.acceptOccurTime(eventHeader.getTimestamp());
    event.parse(buf, pos);

    return event;
  }

  /**
   * 在执行filter时需要表名称和数据库名称，根据名称来判断过滤,此时需要从RowEvent事件中解析
   * 出TableId，然后根据TableId从{@link BinlogContext}中获取{@link TableMapEvent},
   * 由此读取出表的名称。<br>
   * 
   * <b>注意</b><br>
   * <p>
   * 在最初的理解中mysql的binlog按表顺序排列的比如在一个事务中执行了表T1，T2的操作，那么其binlog事件必须是:
   * T1 TableMapEvent + RowEvent
   * T2 TableMapEvent + RowEvent
   * 但事实上会存在如下情况：
   * T1 TableEvent
   * T2 TableEvent
   * T1 RowEvent
   * T2 RowEvent
   * </p>
   * 
   * @param event binlog事件
   * @param buf 当前事件的byte数据
   * @param pos 解析位置
   */
  private void tryParseTableIdOfRowEvent(final BinlogEvent event, final byte[] buf, final Position pos) {
    if (event instanceof BaseRowEvent) {
      BaseRowEvent rowEvent = (BaseRowEvent) event;
      rowEvent.parseTableId(buf, pos);
    }
  }

  /**
   * 事件过滤，根据db、table名称过滤，过滤通过的可以继续解析，否则直接丢弃。仅仅需要解析RowEvent数据
   * 
   * @param event 事件
   * @return 是否过滤通过
   */
  private boolean filter(BinlogEvent event) {
    if (this.tableFilter == null) {
      return true;
    }
    if (event instanceof BaseRowEvent) {
      BaseRowEvent rowEvent = (BaseRowEvent) event;

      boolean ret =
          this.tableFilter.filter(rowEvent.getTableMapEvent().getSchema(), rowEvent.getTableMapEvent().getTableName());

      LOGGER.debug("filter row event,{}.{}, {} ", rowEvent.getTableMapEvent().getSchema(),
          rowEvent.getTableMapEvent().getTableName(), ret);
      if (ret) {
        return true;
      }
      return ret;
    } else {
      return true;
    }
  }

  public TableMetaProvider getTableMetaProvider() {
    return tableMetaProvider;
  }

  public void setTableMetaProvider(TableMetaProvider tableMetaProvider) {
    this.tableMetaProvider = tableMetaProvider;
  }

  public int getServerId() {
    return serverId;
  }

  public void setServerId(int serverId) {
    this.serverId = serverId;
  }

  public TableFilter getTableFilter() {
    return tableFilter;
  }

  public void setTableFilter(TableFilter tableFilter) {
    this.tableFilter = tableFilter;
  }
}
