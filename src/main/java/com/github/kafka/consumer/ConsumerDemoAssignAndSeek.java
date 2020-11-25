package com.github.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
 * Assign and seek basically means replay data or fetch a specific message
 * Here we are not using group Id
 * We don't subscribe to a producer topic
 * We will assign a topic to the consumer
 * We will read value from a required offset
 * 
 */

public class ConsumerDemoAssignAndSeek {
	
	private static Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);
	private static String bootstrapServer = "localhost:9092";
	// private static String groupId = "my-second-java-application";
	private static String topic = "first_topic";
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		// properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		// consumer.subscribe(Arrays.asList(topic));
		
		// Assign a Topic partition
		TopicPartition partition = new TopicPartition(topic, 0); // Reading from partition 0
		// Set an offset for reading position
		long offset = 15L;
		// Assign partition
		consumer.assign(Arrays.asList(partition));
		// Seek messages from partition and offset position
		consumer.seek(partition, offset);
		// Generic logic to limit number or reads
		int numberOfMessagesToRead = 5;
		int numberOfMessagesReadSoFar = 0;
		boolean continueReading = true;
		
		while (continueReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, String> record: records) {
				numberOfMessagesReadSoFar++;
				LOGGER.info("Key: " + record.key() + "\n");
				LOGGER.info("Value: " + record.value() + "\n");
				LOGGER.info("Partition: " + record.partition() + "\n");
				LOGGER.info("Offsets: " + record.offset());
				if(numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
					continueReading = false;
					break;
				}
			}
		}
		LOGGER.info("Exiting the application");
		consumer.close();
	}
}
