package com.hiriver.streamsource.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hiriver.streamsource.impl.TimestampBasedStreamSource.ExtractValidBeginTimestampFunction;
import static com.hiriver.streamsource.impl.TimestampBasedStreamSource.findLastFileStartLeTimestamp;

/**
 * created by Yang Huawei (xander.yhw@alibaba-inc.com) on 2018/10/20 13:05
 */
public class TimestampBasedStreamSourceTest {

    private ExtractValidBeginTimestampFunction extractValidBeginTimestampFunction;
    private List<String> binlogFiles;

    @Before
    public void init() {
        final Map<String, Long> firstEventTimestampMap = new LinkedHashMap<>();
        firstEventTimestampMap.put("mysql-bin.005591", 100L);
        firstEventTimestampMap.put("mysql-bin.005592", 110L);
        firstEventTimestampMap.put("mysql-bin.005593", 120L);
        firstEventTimestampMap.put("mysql-bin.005594", 130L);
        firstEventTimestampMap.put("mysql-bin.005595", 140L);
        firstEventTimestampMap.put("mysql-bin.005596", 150L);

        this.extractValidBeginTimestampFunction = new ExtractValidBeginTimestampFunction() {
            @Override
            public long extract(String binlogFileName) {
                return firstEventTimestampMap.get(binlogFileName);
            }
        };
        this.binlogFiles = new ArrayList<>(firstEventTimestampMap.keySet());

    }

    @Test
    public void testFindLastFileStartLeTimestampGtLastFile() {
        Assert.assertEquals("mysql-bin.005596",
                findLastFileStartLeTimestamp(151L, binlogFiles, extractValidBeginTimestampFunction));
    }

    @Test
    public void testFindLastFileStartLeTimestampEqLastFile() {
        Assert.assertEquals("mysql-bin.005596",
                findLastFileStartLeTimestamp(150L, binlogFiles, extractValidBeginTimestampFunction));
    }

    @Test
    public void testFindLastFileStartLeTimestampEqFirstFile() {
        Assert.assertEquals("mysql-bin.005591",
                findLastFileStartLeTimestamp(100L, binlogFiles, extractValidBeginTimestampFunction));
    }

    @Test
    public void testFindLastFileStartLeTimestampLtFirstFile() {
        Assert.assertEquals(null,
                findLastFileStartLeTimestamp(99L, binlogFiles, extractValidBeginTimestampFunction));
    }

    @Test
    public void testFindLastFileStartLeTimestampEqSecondFile() {
        Assert.assertEquals("mysql-bin.005592",
                findLastFileStartLeTimestamp(110L, binlogFiles, extractValidBeginTimestampFunction));
    }

    @Test
    public void testFindLastFileStartLeTimestampGtFirstFileLtSecondFile() {
        Assert.assertEquals("mysql-bin.005591",
                findLastFileStartLeTimestamp(105L, binlogFiles, extractValidBeginTimestampFunction));
    }
}
