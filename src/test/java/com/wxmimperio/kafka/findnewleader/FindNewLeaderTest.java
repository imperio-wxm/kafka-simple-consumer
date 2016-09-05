package com.wxmimperio.kafka.findnewleader;

import org.junit.Test;

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
}
