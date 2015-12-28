package com.hiriver.unbiz.mysql.lib;

/**
 * 从socket中持续读取数据的抽象描述。
 * <p>
 * 在解析某些响应结果时需要持续的从socket的读取后续数据，这种情况大都发生在解析某些Response的过程之中， 为了让Response不耦合socket对象，定义割爱接口进行隔离
 * </p>
 * 
 * @author hexiufeng
 *
 */
public interface ResultContentReader {
    /**
     * 读取后续的结果数据
     * 
     * @return byte[]数据块
     */
    byte[] readNextPacketPayload();
}
