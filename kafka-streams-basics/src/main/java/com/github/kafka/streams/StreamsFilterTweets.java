package com.github.kafka.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;


public class StreamsFilterTweets {
	
	private static final JsonParser parser = new JsonParser();
	
	public static void main(String[] args) {
		// Create properties
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		
		// Create topology
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> inputTopic =  builder.stream("twitter_tweets");	// Topic to read from
		// Build topology
		KStream<String, String> filteredKStream = inputTopic.filter(
			// Filter for tweets which has a user over 1000+ followers
				(key, jsonString) -> extractFollowersCount(jsonString) > 1000
			);
		filteredKStream.to("important_tweets");

		// Start streams application
		KafkaStreams streams = new KafkaStreams(builder.build(), properties);
		streams.start();
	}
	
	private static Integer extractFollowersCount(String jsonString) {
		try {
			return parser.parse(jsonString).getAsJsonObject().get("user")
					.getAsJsonObject().get("followers").getAsInt();
		} catch (NullPointerException e) {
			return 0;
		}
	}
	
}
