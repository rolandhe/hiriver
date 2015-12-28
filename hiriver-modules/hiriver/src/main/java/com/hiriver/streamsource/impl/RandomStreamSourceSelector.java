package com.hiriver.streamsource.impl;

import java.util.List;

import com.hiriver.streamsource.StreamSource;
import com.hiriver.streamsource.StreamSourceSelector;

public class RandomStreamSourceSelector implements StreamSourceSelector {

    @Override
    public StreamSource select(List<StreamSource> haStreamSourceList) {
        int count = haStreamSourceList.size();
        int index = (int) (Math.random() * count);
        return haStreamSourceList.get(index);
    }

}
