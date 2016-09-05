package com.wxmimperio.kafka.completedemo.serializer;

import com.wxmimperio.kafka.completedemo.model.KafkaBizData;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;


/**
 * 
 * @author SlimRhinoceri
 *
 */
public class DefaultDeserializer{
	
	public static KafkaBizData fromBytes(byte[] value) throws Exception {
		return (KafkaBizData)new ObjectInputStream(new ByteArrayInputStream(value)).readObject();
	}
}
