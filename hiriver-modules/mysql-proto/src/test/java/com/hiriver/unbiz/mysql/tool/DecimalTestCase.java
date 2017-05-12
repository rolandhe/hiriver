package com.hiriver.unbiz.mysql.tool;

import com.hiriver.unbiz.mysql.lib.protocol.tool.MysqlDecimal;

import org.junit.Test;

/**
 * Created by hexiufeng on 2017/5/11.
 */
public class DecimalTestCase {
  @Test
  public void testDecimal(){
    int precision=18;
    int scale = 4;
    byte[] buf = {(byte)0x80,0x0,0x1,0xD,(byte)0xFB,0x38,(byte)0xD2,0x4,(byte)0xD2};
//        byte[] buf = {127, -1, -2, -14, 4, -57, 45, -5, 45};
    MysqlDecimal myDecimal = new MysqlDecimal(precision,scale);
    myDecimal.parse(buf);
    System.out.println(myDecimal.toDecimal());
  }
}
