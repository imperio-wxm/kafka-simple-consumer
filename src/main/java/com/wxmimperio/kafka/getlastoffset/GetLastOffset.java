package com.wxmimperio.kafka.getlastoffset;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.OffsetRequest;

import java.util.HashMap;
import java.util.Map;

/**
 * 现在定义从哪儿开始读取数据
 * Kafka有两个常量可以派上用场：
 * kafka.api.OffsetRequest.EarliestTime()：从日志中找到数据最开始的位置，并从该位置开始读取数据
 * kafka.api.OffsetRequest.LatestTime()：这个只传递新的消息
 * <p>
 * 假使已经有数据了，第一个方法会从头开始读取历史数据；第二个方法则不会读取历史数据，只读取新数据
 * 不要假设起始offset是0，因为随着时间推移，分区中的消息可能会被删除
 * <p>
 * Created by weiximing.imperio on 2016/9/5.
 */

public class GetLastOffset {
    /**
     * 如果要读取最早的数据，在调用getLastOffset方法时:
     *      可以为whichTime赋值为kafka.api.OffsetRequest.EarliestTime()
     * 如果要读取最新的数据:
     *      可以为whichTime赋值为kafka.api.OffsetRequest.LatestTime()
     * @param consumer
     * @param topic
     * @param partition
     * @param whichTime
     * @param clientName
     * @return
     */
    public long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);

        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }
}
