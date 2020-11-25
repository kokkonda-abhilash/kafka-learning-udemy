package com.github.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoCallbackWithKeys {
	private static Logger LOGGER = LoggerFactory.getLogger(ProducerDemoCallbackWithKeys.class);
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
		producer.send(producerRecord, 
	/*
		new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// TODO Auto-generated method stub
			}
		};
	*/
		(metadata, exception) -> {
				if(exception != null) {
					LOGGER.error("Exception occured while pushing data to Kafka: " + exception);
				} else {
					LOGGER.info(
						"Topic: " + metadata.topic() + "\n"
						+ "Offset: " + metadata.offset() + "\n"
						+ "Partition: " + metadata.partition() + "\n"
						+ "Timestamp: " + metadata.timestamp());
				}
			});
		// Close and flush producer
		producer.flush();
		producer.close();
	}
}
