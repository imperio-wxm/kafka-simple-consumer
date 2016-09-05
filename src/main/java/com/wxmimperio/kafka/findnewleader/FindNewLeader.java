package com.wxmimperio.kafka.findnewleader;

import com.wxmimperio.kafka.findleader.FindLeader;
import kafka.javaapi.PartitionMetadata;

import java.util.ArrayList;
import java.util.List;

/**
 * 如果fetchResponse.hasError()返回true，即出现了错误
 * 我们会在日志上记录原因，并关闭consumer，然后尝试找出新的leader
 * Created by weiximing.imperio on 2016/9/5.
 */
public class FindNewLeader {

    public String findNewLeader(String a_oldLeader, int a_port, String a_topic, int a_partition) throws Exception {
        FindLeader findLeader = new FindLeader();
        List<String> seeds = new ArrayList<String>();
        seeds.add(a_oldLeader);

        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader.findLeader(seeds, a_port, a_topic, a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // 第一次通过时，如果leader并没有变化，就留给zookeeper一秒时间进行恢复
                // 第二次，假如broker已经恢复过来，就可能不是Broker的问题
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }
}
