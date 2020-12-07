package com.github.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
	// REF: https://kafka.apache.org/documentation/#producerconfigs
	public static void main(String[] args) {
		String bootstrapServerString = "localhost:9092";
		// Create configurations
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerString);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// Create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		// Create producer data
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "Hello world!");
		// send data
		producer.send(producerRecord);
		// Close and flush producer
		producer.flush();
		producer.close();
	}
}
