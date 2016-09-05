package com.wxmimperio.kafka.findleader;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 获取分区元数据信息
 * Created by weiximing.imperio on 2016/9/5.
 */

public class FindLeader {
    private List<String> m_replicaBrokers = new ArrayList<String>();

    public PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
        //partition元数据
        PartitionMetadata returnMetaData = null;
        List<String> topics = Collections.singletonList(a_topic);

        boolean find = false;

        for (String seed : a_seedBrokers) {
            SimpleConsumer leaderSearcher = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");
            try {
                //请求和响应
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                TopicMetadataResponse resp = leaderSearcher.send(req);

                //topicsMetadata 通过这个方法，程序可以向已经连接的Broker请求关于目标topic的全部细节
                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            find = true;
                            break;
                        }
                        //找到对应partition直接break
                        if (find) {
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
                        + ", " + a_partition + "] Reason: " + e);
            } finally {
                leaderSearcher.close();
            }
            if (find) {
                break;
            }
        }
        //记录了topic所有副本所在的broker，如果需要重新找出新的leader，则需要这些记录
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }
}
