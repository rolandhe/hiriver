package com.hiriver.unbiz.mysql.lib.protocol.tool;

import com.hiriver.unbiz.mysql.lib.ColumnType;

/**
 * Created by hexiufeng on 2017/5/28.
 */
public class GenericStringTypeChecker {
  private GenericStringTypeChecker(){

  }

  /**
   * 针对于char类型所做的特殊处理，主要是重新修正length，参见log_event.cc#log_event_print_value
   *
   * @param meta column meta value
   * @param lengthHolder length占位符
   * @return
   */
  public static ColumnType checkRealColumnType(int meta, int[] lengthHolder) {
    // from log_event.cc
    if (meta >= 256) {
      // column type
      int byte0 = meta >>> 8;
      // length
      int byte1 = meta & 0xff;

      if ((byte0 & 0x30) != 0x30) {
        /* a long CHAR() field: see #37426 */
        lengthHolder[0] = byte1 | (((byte0 & 0x30) ^ 0x30) << 4);

        return ColumnType.ofTypeValue(byte0 | 0x30);
      }

      lengthHolder[0] = meta & 0xff;
      return ColumnType.ofTypeValue(byte0);

    }
    lengthHolder[0] = meta;
    return ColumnType.MYSQL_TYPE_STRING;
  }
}
