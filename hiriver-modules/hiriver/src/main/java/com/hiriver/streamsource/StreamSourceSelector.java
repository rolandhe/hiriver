package com.hiriver.streamsource;

import java.util.List;

/**
 * 在HA数据源场景下，选择数据源的策略
 * 
 * @author hexiufeng
 *
 */
public interface StreamSourceSelector {
    /**
     * 从备选数据源中选定一个数据源
     * 
     * @param haStreamSourceList 备选数据源
     * @return 选定的数据源
     */
    StreamSource select(final List<StreamSource> haStreamSourceList);
}
