package com.wxmimperio.kafka.findnewleader;

import com.wxmimperio.kafka.findleader.FindLeader;
import kafka.javaapi.PartitionMetadata;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by weiximing.imperio on 2016/9/5.
 */
public class FindNewLeader2 {

    public String getLeaderBroderName() {

        FindLeader findLeader = new FindLeader();
        List<String> seeds = new ArrayList<String>();
        seeds.add("192.168.18.35");

        PartitionMetadata partitionMeta = null;
        int times = 0;
        while (partitionMeta == null || partitionMeta.leader() == null) {
            if (times == 5) {
                throw new IllegalStateException("试了5次也没有找到leader，退出了");
            } else if (times != 0) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                partitionMeta = findLeader.findLeader(seeds, 9092, "topic", 0);
                times++;
            }
        }
        return partitionMeta.leader().host();
    }
}
