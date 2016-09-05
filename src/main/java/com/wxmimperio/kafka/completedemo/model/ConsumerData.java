package com.wxmimperio.kafka.completedemo.model;

import java.util.List;

/**
 * 消费到的数据对象
 * @author SlimRhinoceri
 */
public class ConsumerData {
	
	private long offset;
	
	private List<KafkaBizData> datas;

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public List<KafkaBizData> getDatas() {
		return datas;
	}

	public void setDatas(List<KafkaBizData> datas) {
		this.datas = datas;
	}
	
	public String toString(){
		StringBuilder builder = new StringBuilder();
		builder.append("当前offset:" + this.offset);
		builder.append("\n");
		for(int i = 0, len = this.datas.size(); i < len; i++){
			builder.append(this.datas.get(i));
			builder.append("\n");
		}
		
		return builder.toString();
	}
}
