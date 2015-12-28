package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;

public interface ColumnTypeValueParser {
    Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta);
}
