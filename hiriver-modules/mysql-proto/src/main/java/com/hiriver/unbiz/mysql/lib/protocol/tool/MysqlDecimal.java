package com.hiriver.unbiz.mysql.lib.protocol.tool;

import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;

import java.math.BigDecimal;

/**
 * 用于描述、解析mysql binlog格式的decimal类型数据。该逻辑来自mysql源码中的 decimal.c, decimal2bin and bin2decimal. <br>
 *
 *
 * Created by hexiufeng on 2017/5/11.
 */
public class MysqlDecimal {
  /**
   * 负号
   */
  private  static  final char NEGATIVE_SIGN = '-';
  /**
   * 小数点
   */
  private  static  final char DECIMAL_POINT = '.';

  /**
   * 字符'0'所对应的Ascii码值，用于转化0-9数值到'0'-'9'字符
   */
  private static final int ZERO_ASCII = 48;

  /**
   * int 数据类型所占字节数
   */
  private static final int SIZE_OF_INT32 = 4;
  /**
   * decimal 按位分段基数
   */
  private static final int DIG_PER_DEC1 = 9;

  /**
   * 分段后，不足一段的部分转化为binary时所应该占的字节数据, 不足一段的长度作为下标，<br>
   * 比如：1234872356870.12, 整数部分分为1234 872356870, 1234就是不足一段的部分,长度<br>
   * 是4，那么它所占的字节数是DIG2BYTES[4]=2
   */
  private static final int[] DIG2BYTES = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };
  /**
   * decimal使用内部使用int[] 来存储，每个int即调拨一个段，每个段为1-9位，POWERS10用于描述不同
   * 的位数所能表达的上限值，注意是开区间
   */
  private static final int POWERS10[] = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };

  /**
   * decimal整数部分的十进制长度
   */
  private final int intLength;
  /**
   * decimal小数部分的十进制长度
   */
  private final int fracLength;
  /**
   * decimal 整数部分的分段数, 每一段的长度是 {@link DIG_PER_DEC1}
   */
  private final int intDecSize;
  /**
   * decimal 小数部分的分段数, 每一段的长度是 {@link DIG_PER_DEC1}
   */
  private final int fracDecSize;
  /**
   * decimal 整数部分不足一段十进制长度
   */
  private final int intNoneDecLength;
  /**
   * decimal 小数数部分不足一段十进制长度
   */
  private final int fracNoneDecLength;
  /**
   * 指定精度和小数位数的decimal转化成binary后的长度
   */
  private final int binSize;
  /**
   * 整数部分用数组表示
   */
  private final int[] intArray;
  /**
   * 小数部分用数组表示
   */
  private final int[] fracArray;

  /**
   * 符号，false 表示正数
   */
  private boolean sign;



  /**
   * 根据指定的精度和小数位数生成decimal的描述信息
   *
   * @param precision  精度
   * @param scale 小数位数
   */
  public MysqlDecimal(int precision, int scale){
    intLength = precision - scale;
    fracLength = scale;
    intDecSize = intLength / DIG_PER_DEC1;
    fracDecSize = fracLength / DIG_PER_DEC1;
    intNoneDecLength = intLength % DIG_PER_DEC1;
    fracNoneDecLength = fracLength % DIG_PER_DEC1;
    binSize = intDecSize * SIZE_OF_INT32 + fracDecSize * SIZE_OF_INT32 + DIG2BYTES[intNoneDecLength] +
            DIG2BYTES[fracNoneDecLength];
    intArray = new int[(intLength + DIG_PER_DEC1 - 1)/DIG_PER_DEC1];
    fracArray = new int[(fracLength + DIG_PER_DEC1 - 1)/DIG_PER_DEC1];
  }

  /**
   * 由二进制数值解析成decimal的内部存储
   *
   * @param buf 二进制值
   */
  public void parse(byte[] buf){
    Position pos = Position.factory();
    sign = (buf[0] & 0x80) == 0;
    buf[0] ^= 0x80;
    int mask = sign?-1:0;
    for(int i = 0; i < buf.length;i++){
      buf[i] ^= mask;
    }
    parseIntSection(buf, pos);

    parseFracSection(buf, pos);

  }

  /**
   * 解析小数部分
   *
   * @param buf
   * @param pos
   */
  private void parseFracSection(byte[] buf, Position pos) {
    for(int i = 0; i < fracDecSize;i++){
      fracArray[i] = readInt(buf,pos,SIZE_OF_INT32);
      checkIntValue(intArray[i],DIG_PER_DEC1);
    }
    if(fracNoneDecLength > 0){
      fracArray[fracArray.length - 1] = readInt(buf,pos,DIG2BYTES[fracNoneDecLength]);
      checkIntValue(fracArray[0],fracNoneDecLength);
    }
  }

  /**
   * 解析整数部分
   *
   * @param buf
   * @param pos
   */
  private void parseIntSection(byte[] buf, Position pos) {
    int startIndex = 0;
    if(intNoneDecLength > 0){
      intArray[0] = readInt(buf,pos,DIG2BYTES[intNoneDecLength]);
      checkIntValue(intArray[0],intNoneDecLength);
      startIndex++;
    }
    for(int i = 0; i < intDecSize;i++){
      intArray[startIndex+i] = readInt(buf,pos,SIZE_OF_INT32);
      checkIntValue(intArray[startIndex+i],DIG_PER_DEC1);
    }
  }

  /**
   * 转化成BigDecimal
   *
   * @return
   */
  public BigDecimal toDecimal() {
    SimpleStringBuilder sb = new SimpleStringBuilder(calDecimalStringLength());
    if(sign){
      sb.append(NEGATIVE_SIGN);
    }
    convertIntSection2CharArray(sb);
    convertFracSection2CharArray(sb);
    return new BigDecimal(sb.toCharArray());
  }

  /**
   * 转化小数部分为字符数组
   *
   * @param sb
   */
  private void convertFracSection2CharArray(SimpleStringBuilder sb) {
    if(fracLength > 0){
      sb.append(DECIMAL_POINT);
    }
    for(int i = 0; i < fracDecSize;i++){
      convertInt2Char(fracArray[i],DIG_PER_DEC1,sb);
    }
    if(fracNoneDecLength > 0){
      convertInt2Char(fracArray[fracArray.length - 1],fracNoneDecLength,sb);
    }
  }

  /**
   * 转化整数部分为字符数组
   *
   * @param sb
   */
  private void convertIntSection2CharArray(SimpleStringBuilder sb) {
    int startIndex = 0;
    if(intNoneDecLength > 0){
      convertInt2Char(intArray[0],intNoneDecLength,sb);
      startIndex++;
    }
    for(int i = 0; i < intDecSize;i++){
      convertInt2Char(intArray[startIndex + i],DIG_PER_DEC1,sb);
    }
  }

  public  int getBinSize(){
    return this.binSize;
  }

  /**
   * 计算decimal转化成字符数组的长度
   *
   * @return
   */
  private int calDecimalStringLength(){
    int len = intLength;
    // 符号位
    if(sign){
      len++;
    }
    // 如果有小数，需要小数点
    if(fracLength > 0){
      len += fracLength + 1;
    }
    return len;
  }

  /**
   * 把一个整数转化成指定长度的char数组，如果整数的总位数小于指定的长度，在高位用0补齐
   *
   * @param value
   * @param digitLen
   * @param sb
   */
  private void convertInt2Char(int value,int digitLen, final SimpleStringBuilder sb){
    for(int i = digitLen - 1; i >=0;i--){
      int digit = value / POWERS10[i];
      digit += ZERO_ASCII;
      sb.append((char)digit);
      value = value % POWERS10[i];
    }
  }

  /**
   * 根据指定的字节数读取int值，
   *
   * @param buf 大尾端描述
   * @param pos
   * @param byteSize 必须是 1、2、3、4中的一个
   * @return
   */
  private int readInt(final byte[] buf, final Position pos, final int byteSize){
    switch (byteSize){
      case 1:
        return MysqlNumberUtils.read1Int(buf, pos);
      case 2:
        return MysqlNumberUtils.read2BEInt(buf, pos);
      case 3:
        return MysqlNumberUtils.read3BEInt(buf,pos);
      case 4:
        return MysqlNumberUtils.read4BEInt(buf,pos);
      default:
        throw new RuntimeException("read int value error: int must be 1,2,3,4 bytes.");
    }
  }

  /**
   * 检查用于描述decimal的int[]中的每个数值是有效的
   *
   * @param value
   * @param digitLen
   */
  private void checkIntValue(int value, int digitLen){
    if(value >= POWERS10[digitLen]){
      throw new RuntimeException("invalid decimal int value:"+value);
    }
  }

  /**
   * 简单的用于拼接字符的容器，指定固定的长度，顺序拼接，
   * 一次初始化内部存储，不动态扩容，也不复制
   *
   */
  private  static  class SimpleStringBuilder{
    private  final char[] buf;
    private int position;
    SimpleStringBuilder(int capcity){
      buf = new char[capcity];
    }
    void append(char c){
      buf[position++] = c;
    }
    char[] toCharArray(){
      return buf;
    }
  }
  public  static  void  main(String[] args){
    int precision=18;
    int scale = 4;
    byte[] buf = {(byte)0x80,0x0,0x1,0xD,(byte)0xFB,0x38,(byte)0xD2,0x4,(byte)0xD2};
//        byte[] buf = {127, -1, -2, -14, 4, -57, 45, -5, 45};
    MysqlDecimal myDecimal = new MysqlDecimal(precision,scale);
    myDecimal.parse(buf);
    System.out.println(myDecimal.toDecimal());
  }
}
