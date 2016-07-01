package com.hiriver.unbiz.mysql.lib.protocol.binlog.extra;

import com.hiriver.unbiz.mysql.lib.protocol.Request;

/**
 * 描述mysql binlog的同步点。当一个事务结束时需要记录同步点，当重新同步时可以从该同步点继续同步。<br>
 * 
 * <ul>
 * <li>mysql5.6.9之前，同步点是binlog file name + offset</li>
 * <li>mysql5.6.9之后，同步点可以是gtid，当从mysql从库复制数据时，当一个从库崩溃，可以自动切换到其他从库，此时 gtid特别有用，可以保证从正确的位置继续复制，但第一种方式不行</li>
 * </ul>
 * 
 * @author hexiufeng
 *
 */
public interface BinlogPosition {
    /**
     * 转换binlog pos为dump指令
     * 
     * @param serverId 从库的唯一id，当前系统逻辑上就是一个从库
     * @param extendGtId 当前数据库的gtid
     * @return dump指令请求
     */
    Request packetDumpRequest(int serverId,String extendGtId);
    
    /**
     * 转换成可以存储的二进制流。缺省实现是调用当前对象的toString()方法，然后转换成byte数组
     * 
     * @return byte 数组
     */
    byte[] toBytesArray();

    /**
     * 判断两个 pos是否相同
     * 
     * @param pos 所比较的位置
     * @return 是否相同
     */
    boolean isSame(BinlogPosition pos);
}
