package com.hiriver.streamsource.impl;

import java.util.List;

import com.hiriver.streamsource.StreamSource;
import com.hiriver.streamsource.StreamSourceSelector;

/**
 * 基于随机算法的HA数据源选择策略实现。
 * 线程安全
 * 
 * @author hexiufeng
 *
 */
public class RandomStreamSourceSelector implements StreamSourceSelector {

    @Override
    public StreamSource select(List<StreamSource> haStreamSourceList) {
        int count = haStreamSourceList.size();
        int index = (int) (Math.random() * count);
        return haStreamSourceList.get(index);
    }

}
