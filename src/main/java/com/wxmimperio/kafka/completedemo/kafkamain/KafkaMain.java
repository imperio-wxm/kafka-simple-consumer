package com.wxmimperio.kafka.completedemo.kafkamain;

import com.wxmimperio.kafka.completedemo.kafkaconsumer.KafkaConsumer;
import com.wxmimperio.kafka.completedemo.model.ConsumerData;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by weiximing.imperio on 2016/9/5.
 */
public class KafkaMain implements Runnable {

    private static final String QUIT = "quit";

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        KafkaMain r = new KafkaMain();
        new Thread(r).start();

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        @SuppressWarnings("unused")
        String cmd = null;
        while (QUIT.equals(cmd = reader.readLine()) == false) {
        }

        System.exit(0);
    }

    @Override
    public void run() {

        List<String> brokers = new ArrayList<String>();
        //brokers.add("10.1.8.207:9092");
        //brokers.add("10.1.8.206:9092");
        //brokers.add("10.1.8.208:9092");
        brokers.add("192.168.18.35:9092");
        //从当前后一个开始读取消息
        long curOffset = getOffset() + 1;
        while (true) {
            KafkaConsumer c = null;
            try {
                c = new KafkaConsumer("topic_001", 0, curOffset, brokers);
                c.open();
                ConsumerData data = c.fetch();
                if (data != null) {
                    curOffset = data.getOffset();
                    //byte[] message = data.getDatas();
                    System.out.println(data.getOffset());
                    for (byte[] message : data.getDatas()) {
                        System.out.println("======" + new String(message));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (c != null) {
                    c.close();
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private long getOffset() {
        Map<String, Integer> brokers = new HashMap<String, Integer>();
        List<String> brokerAddrs = new ArrayList<String>();
        brokerAddrs.add("192.168.18.35:9092");

        long retrievedOffset = 0;


        for (String addr : brokerAddrs) {
            String[] arr = addr.split(":");
            brokers.put(arr[0], Integer.parseInt(arr[1]));
        }

        List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
        TopicAndPartition testPartition0 = new TopicAndPartition("topic_001", 0);

        for (String broker : brokers.keySet()) {

            SimpleConsumer leaderSearcher = new SimpleConsumer(broker, brokers.get(broker), 100000, 64 * 1024, "leaderLookup");

            partitions.add(testPartition0);
            OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                    null,
                    partitions,
                    kafka.api.OffsetRequest.CurrentVersion() /* version */, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
                    0,
                    "testClient");
            OffsetFetchResponse fetchResponse = leaderSearcher.fetchOffsets(fetchRequest);
            OffsetMetadataAndError result = fetchResponse.offsets().get(testPartition0);
            short offsetFetchErrorCode = result.error();

            if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
                System.out.println("NotCoordinatorForConsumerCode");
                // Go to step 1 and retry the offset fetch
            } else {
                retrievedOffset = result.offset();
                System.out.println(retrievedOffset);
            }
            leaderSearcher.close();
        }
        return retrievedOffset;
    }
}
