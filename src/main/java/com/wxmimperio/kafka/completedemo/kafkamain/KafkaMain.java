package com.wxmimperio.kafka.completedemo.kafkamain;

import com.wxmimperio.kafka.completedemo.kafkaconsumer.KafkaConsumer;
import com.wxmimperio.kafka.completedemo.model.ConsumerData;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
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

        List<String> brokers = new ArrayList<String>();
        //brokers.add("10.1.8.207:9092");
        //brokers.add("10.1.8.206:9092");
        //brokers.add("10.1.8.208:9092");
        brokers.add("192.168.18.35:9092");
        long curOffset = 100;
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
}
