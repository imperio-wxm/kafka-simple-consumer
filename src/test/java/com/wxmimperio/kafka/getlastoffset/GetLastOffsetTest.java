package com.wxmimperio.kafka.getlastoffset;

import com.wxmimperio.kafka.findleader.FindLeader;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        PartitionMetadata metadata = findLeader.findLeader(seeds, 9092, "low_level_topic_01", 2);

        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + "topic_001" + "_" + 0;

        SimpleConsumer consumer = new SimpleConsumer(leadBroker, 9092, 100000, 64 * 1024, clientName);
        GetLastOffset getLastOffset = new GetLastOffset();
        long lastOffset = getLastOffset.getLastOffset(consumer, "low_level_topic_01", 2,
                kafka.api.OffsetRequest.LatestTime(), clientName);

        System.out.println(lastOffset);
    }

    @Test
    public void fetchOffset() {
        Map<String, Integer> brokers = new HashMap<String, Integer>();
        List<String> brokerAddrs = new ArrayList<String>();
        brokerAddrs.add("192.168.18.35:9092");

        for (String addr : brokerAddrs) {
            String[] arr = addr.split(":");
            brokers.put(arr[0], Integer.parseInt(arr[1]));
        }

        List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
        TopicAndPartition testPartition0 = new TopicAndPartition("low_level_topic_01", 0);

        for (String broker : brokers.keySet()) {

            SimpleConsumer leaderSearcher = new SimpleConsumer(broker, brokers.get(broker), 100000, 64 * 1024, "leaderLookup");

            partitions.add(testPartition0);
            OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                    null,
                    partitions,
                    kafka.api.OffsetRequest.CurrentVersion() /* version */, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
                    2000,
                    "testClient");
            OffsetFetchResponse fetchResponse = leaderSearcher.fetchOffsets(fetchRequest);
            OffsetMetadataAndError result = fetchResponse.offsets().get(testPartition0);
            short offsetFetchErrorCode = result.error();

            if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
                System.out.println("NotCoordinatorForConsumerCode");
                // Go to step 1 and retry the offset fetch
            } else {
                long retrievedOffset = result.offset();
                System.out.println(retrievedOffset);
            }
            leaderSearcher.close();
        }
    }
}
