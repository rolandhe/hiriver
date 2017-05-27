package com.hiriver.unbiz.mysql.lib.protocol.text;

import com.hiriver.unbiz.mysql.lib.CharsetMapping;
import com.hiriver.unbiz.mysql.lib.ColumnType;
import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.AbstractResponse;
import com.hiriver.unbiz.mysql.lib.protocol.ColumnFlagConst;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.Response;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;
import com.hiriver.unbiz.mysql.lib.protocol.tool.StringTool;

/**
 * 文本协议，返回列定义的reponse对象，对应{@link TextCommandFieldListRequest}的响应信息
 * 
 * @author hexiufeng
 *
 */
public class ColumnDefinitionResponse extends AbstractResponse implements Response {
    private String catalog;
    private String schema;
    private String table;
    private String orgTable;
    private String name;
    private String orgName;
    private int nextLength; // 0x0c
    private String charset;
    private int columnLength;
    private ColumnType type;
    private int flags;
    private int decimals;
    private byte[] filler = new byte[2];
    // for TextCommandFieldList
    private int defValueLen;
    private byte[] defValue;

    private final boolean isQuery;

    public ColumnDefinitionResponse() {
        this.isQuery = true;
    }

    public ColumnDefinitionResponse(boolean isQuery) {
        this.isQuery = isQuery;
    }

    @Override
    public void parse(byte[] buf) {
        Position pos = Position.factory();
        this.catalog = getLencString(buf, pos);
        this.schema = getLencString(buf, pos);
        this.table = getLencString(buf, pos);
        this.orgTable = getLencString(buf, pos);
        this.name = getLencString(buf, pos);
        this.orgName = getLencString(buf, pos);
        nextLength = (int) MysqlNumberUtils.readLencodeLong(buf, pos);

        charset = CharsetMapping.getJavaEncodingForCharsetValue(MysqlNumberUtils.read2Int(buf, pos));
        columnLength = MysqlNumberUtils.read4Int(buf, pos);

        type = ColumnType.ofTypeValue(MysqlNumberUtils.read1Int(buf, pos));

        flags = MysqlNumberUtils.read2Int(buf, pos);

        decimals = MysqlNumberUtils.read1Int(buf, pos);

        // skip filler
        pos.forwardPos(2);

        if (isQuery) {
            return;
        }
        if (MysqlNumberUtils.isValidLencodeLong(buf, pos)) {
            defValueLen = (int) MysqlNumberUtils.readLencodeLong(buf, pos);
            defValue = MysqlStringUtils.readFixString(buf, pos, defValueLen);
        }
    }

    public ColumnDefinition toColumnDefinition() {
        ColumnDefinition def = new ColumnDefinition();
        def.setCharset(this.charset);
        def.setColumName(this.name);
        def.setType(this.type);
        def.setLen(this.columnLength);
        return def;
    }

    public boolean isNull() {
        return !flagsContains(ColumnFlagConst.NOT_NULL_FLAG);
    }

    public boolean isPrimayKey() {
        return flagsContains(ColumnFlagConst.PRI_KEY_FLAG);
    }

    public boolean isUnsigned() {
        return flagsContains(ColumnFlagConst.UNSIGNED_FLAG);
    }

    public boolean isUniqueKey() {
        return flagsContains(ColumnFlagConst.UNIQUE_KEY_FLAG);
    }

    public boolean isKey() {
        return flagsContains(ColumnFlagConst.MULTIPLE_KEY_FLAG);
    }

    private boolean flagsContains(int columnFlag) {
        return (flags & columnFlag) != 0;
    }

    private String getLencString(byte[] buf, Position pos) {
        int lenc = (int) MysqlNumberUtils.readLencodeLong(buf, pos);
        byte[] tb = MysqlStringUtils.readFixString(buf, pos, lenc);
        return StringTool.safeConvertBytes2String(tb);
    }

    public String getCatalog() {
        return catalog;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getOrgTable() {
        return orgTable;
    }

    public String getName() {
        return name;
    }

    public String getOrgName() {
        return orgName;
    }

    public int getNextLength() {
        return nextLength;
    }

    public String getCharset() {
        return charset;
    }

    public int getColumnLength() {
        return columnLength;
    }

    public ColumnType getType() {
        return type;
    }

    public int getFlags() {
        return flags;
    }

    public int getDecimals() {
        return decimals;
    }

    public byte[] getFiller() {
        return filler;
    }

    public int getDefValueLen() {
        return defValueLen;
    }

    public byte[] getDefValue() {
        return defValue;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setOrgTable(String orgTable) {
        this.orgTable = orgTable;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setOrgName(String orgName) {
        this.orgName = orgName;
    }

    public void setNextLength(int nextLength) {
        this.nextLength = nextLength;
    }

    public void setColumnLength(int columnLength) {
        this.columnLength = columnLength;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public void setDecimals(int decimals) {
        this.decimals = decimals;
    }

    public void setFiller(byte[] filler) {
        this.filler = filler;
    }

    public void setDefValueLen(int defValueLen) {
        this.defValueLen = defValueLen;
    }

    public void setDefValue(byte[] defValue) {
        this.defValue = defValue;
    }

}
