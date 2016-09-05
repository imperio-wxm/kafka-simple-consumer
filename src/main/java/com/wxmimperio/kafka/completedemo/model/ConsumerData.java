package com.wxmimperio.kafka.completedemo.model;

import java.util.List;

/**
 * 消费到的数据对象
 *
 * @author SlimRhinoceri
 */
public class ConsumerData {

    private long offset;

    private List<byte[]> datas;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public List<byte[]> getDatas() {
        return datas;
    }

    public void setDatas(List<byte[]> datas) {
        this.datas = datas;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("当前offset:").append(this.offset);
        builder.append("\n");
        for (byte[] data : this.datas) {
            builder.append(new String(data));
            builder.append("\n");
        }

        return builder.toString();
    }
}
