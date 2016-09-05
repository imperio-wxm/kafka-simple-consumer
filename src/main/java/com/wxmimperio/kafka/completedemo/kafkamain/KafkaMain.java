package com.wxmimperio.kafka.completedemo.kafkamain;

import com.wxmimperio.kafka.completedemo.kafkaconsumer.KafkaConsumer;
import com.wxmimperio.kafka.completedemo.model.ConsumerData;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;

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

        List<String> brokers = Collections.singletonList("192.168.18.35:9092");
        long curOffset = -1;
        while (true) {
            KafkaConsumer c = null;
            try {
                c = new KafkaConsumer("topic_1", 0, curOffset, brokers);
                c.open();
                ConsumerData data = c.fetch();
                curOffset = data.getOffset();
                System.out.println(data);
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
}
