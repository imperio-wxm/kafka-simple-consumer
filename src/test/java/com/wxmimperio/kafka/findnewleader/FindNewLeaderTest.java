package com.wxmimperio.kafka.findnewleader;

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

        PartitionMetadata partitionMeta = null;
        List<String> topics = Collections.singletonList("topic_002");

        boolean find = false;
        for (String broker : brokers.keySet()) {
            SimpleConsumer leaderSearcher = new SimpleConsumer(broker, brokers.get(broker), 100000, 64 * 1024, "leaderLookup");
            try {
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = leaderSearcher.send(req);
                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        partitionList.add(part.partitionId());
                        /*if (part.partitionId() == this.partition) {
                            partitionMeta = part;
                            find = true;
                            break;
                        }*/
                    }
                    /*if (find) {
                        break;
                    }*/
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                leaderSearcher.close();
            }
            if (find) {
                break;
            }
        }

        for(int partitonId : partitionList) {
            System.out.println("partition id = " + partitonId);
        }
    }
}
