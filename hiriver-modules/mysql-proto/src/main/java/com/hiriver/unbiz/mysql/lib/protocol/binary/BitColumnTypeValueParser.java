package com.hiriver.unbiz.mysql.lib.protocol.binary;

import com.hiriver.unbiz.mysql.lib.output.ColumnDefinition;
import com.hiriver.unbiz.mysql.lib.protocol.Position;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlNumberUtils;
import com.hiriver.unbiz.mysql.lib.protocol.datautils.MysqlStringUtils;

/**
 * bit类型的数据解析器
 * 
 * @author hexiufeng
 *
 */
public class BitColumnTypeValueParser implements ColumnTypeValueParser {

    @Override
    public Object parse(byte[] buf, Position pos, ColumnDefinition columnDef, int meta) {
        int nbits = ((meta >>> 8) * 8) + (meta & 0xff);
        int len = (nbits + 7) / 8;
        byte[] bitBuff = MysqlStringUtils.readFixString(buf, pos, len);
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < bitBuff.length;i++){
            int v = bitBuff[i] & 0xff;
            if(i == 0) {
                sb.append(Integer.toBinaryString(v));
                continue;
            }
            String pad = "00000000" + Integer.toBinaryString(v);
            sb.append(pad.substring(pad.length() - 8));
        }
        return "0b" + sb.toString();
    }

}
