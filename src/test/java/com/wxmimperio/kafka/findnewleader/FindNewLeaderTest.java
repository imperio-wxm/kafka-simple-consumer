package com.wxmimperio.kafka.findnewleader;

import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.junit.Test;

import java.util.*;

/**
 * Created by weiximing.imperio on 2016/9/5.
 */
public class FindNewLeaderTest {

    @Test
    public void findNewLeaderTest() {
        FindNewLeader findNewLeader = new FindNewLeader();
        try {
            String newLeader = findNewLeader.findNewLeader("192.168.18.35", 9092, "topic", 0);
            System.out.println(newLeader);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void findNewLeaderTest2() {
        FindNewLeader2 findNewLeader2 = new FindNewLeader2();
        String newLeader = findNewLeader2.getLeaderBroderName();
        System.out.println(newLeader);
    }

    @Test
    public void getPartitionMetadata() {

        Map<String, Integer> brokers = new HashMap<String, Integer>();
        List<String> brokerAddrs = new ArrayList<String>();
        brokerAddrs.add("192.168.18.35:9092");

        List<Integer> partitionList = new ArrayList<Integer>();

        for (String addr : brokerAddrs) {
            String[] arr = addr.split(":");
            brokers.put(arr[0], Integer.parseInt(arr[1]));
        }

        List<String> topics = Collections.singletonList("topic_001");

        int correlationId = 0;

        for (String broker : brokers.keySet()) {
            SimpleConsumer leaderSearcher = new SimpleConsumer(broker, brokers.get(broker), 100000, 64 * 1024, "leaderLookup");
            try {

                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = leaderSearcher.send(req);


                OffsetCommitRequest offsetCommitRequest = commitOffset(correlationId);
                kafka.javaapi.OffsetCommitResponse offsetResp = leaderSearcher.commitOffsets(offsetCommitRequest);

                if (offsetResp.hasError()) {
                    for (Object partitionErrorCode : offsetResp.errors().values()) {
                        if ((Short) partitionErrorCode == ErrorMapping.OffsetMetadataTooLargeCode()) {
                            // You must reduce the size of the metadata if you wish to retry
                            System.out.println("OffsetMetadataTooLargeCode");
                        } else if ((Short) partitionErrorCode == ErrorMapping.NotCoordinatorForConsumerCode() || (Short) partitionErrorCode == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
                            System.out.println("NotCoordinatorForConsumerCode");
                        }
                    }
                }

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        partitionList.add(part.partitionId());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                leaderSearcher.close();
            }
        }

        for (int partitonId : partitionList) {
            System.out.println("partition id = " + partitonId);
        }
    }

    private OffsetCommitRequest commitOffset(int correlationId) {
        TopicAndPartition topicAndPartition = new TopicAndPartition("topic_002", 0);

        Map<TopicAndPartition, OffsetAndMetadata> offsets = new LinkedHashMap<TopicAndPartition, OffsetAndMetadata>();

        for (int i = 0; i < 100; i++) {
            long now = System.currentTimeMillis();
            offsets.put(topicAndPartition, new OffsetAndMetadata(i, "more metadata", now));
        }

        /*long now = System.currentTimeMillis();
        offsets.put(topicAndPartition, new OffsetAndMetadata(-1, "more metadata", now));*/

        return new OffsetCommitRequest(null, offsets, 0, "testClient");
    }
}
