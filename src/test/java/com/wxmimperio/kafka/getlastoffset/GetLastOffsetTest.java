package com.wxmimperio.kafka.getlastoffset;

import com.wxmimperio.kafka.findleader.FindLeader;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by weiximing.imperio on 2016/9/5.
 */
public class GetLastOffsetTest {

    @Test
    public void getLastOffsetTest() {
        //结合findLeader找到集群leader
        FindLeader findLeader = new FindLeader();
        List<String> seeds = new ArrayList<String>();
        seeds.add("192.168.18.35");
        PartitionMetadata metadata = findLeader.findLeader(seeds, 9092, "topic_1", 0);

        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + "topic_1" + "_" + 0;

        SimpleConsumer consumer = new SimpleConsumer(leadBroker, 9092, 100000, 64 * 1024, clientName);
        GetLastOffset getLastOffset = new GetLastOffset();
        long lastOffset = getLastOffset.getLastOffset(consumer, "topic_1", 0,
                kafka.api.OffsetRequest.LatestTime(), clientName);

        System.out.println(lastOffset);
    }
}
