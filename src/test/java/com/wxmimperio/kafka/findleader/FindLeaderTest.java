package com.wxmimperio.kafka.findleader;

import kafka.javaapi.PartitionMetadata;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by weiximing.imperio on 2016/9/5.
 */
public class FindLeaderTest {

    @Test
    public void findLeaderTest() {
        FindLeader findLeader = new FindLeader();
        List<String> seeds = new ArrayList<String>();
        seeds.add("192.168.18.35");
        PartitionMetadata metadata = findLeader.findLeader(seeds, 9092, "topic_1", 0);
        System.out.println(metadata.toString());
    }
}
