package com.github.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import sun.util.logging.resources.logging;

public class TwitterProducer {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class.getName());
	private String cosumerKey = "jrNeOtpihMCgD5RkbXVtMIFSx";
	private String consumerKeySecret = "WWmNFHNBh9M5e8Oo5Gcy2jYFO2RiOiJzakkhvVV7QRQMaJaIE3";
	private String token = "1069219397410054145-mUELR7iXbH70TyrZyyr186Rz4paZNW";
	private String tokenSecret = "aEntW7KysVfCv4mRhGw3M8qoo8d3FMiWVvaa6GfbldwNr";

	public static void main(String[] args) {
		new TwitterProducer().run();
	}
	
	public void run() {
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		// Create a twitter client
		Client client = getKafkaClient(msgQueue);
		// Attempts to establish a connection.
		client.connect();
		// Create a Kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			LOGGER.info("Sutting down the application...");
			LOGGER.info("Stopping Kafka client");
			client.stop();
			LOGGER.info("Stopping Kafka producer");
			producer.close();
			LOGGER.info("Done!");
		}));

		// loop to send tweets to Kafka
		while (!client.isDone()) {
			String message = null;
			try {
				message = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if(message != null) {
				producer.send(new ProducerRecord<>("twitter_tweet", null, message), (recordMetaData, exception) -> {
					if (exception != null) {
						LOGGER.error("Something bad happened, " + exception.getMessage());
						exception.printStackTrace();
					} else {
						LOGGER.info(recordMetaData.topic());
						LOGGER.info(String.valueOf(recordMetaData.partition()));
						LOGGER.info(String.valueOf(recordMetaData.offset()));
						LOGGER.info(String.valueOf(recordMetaData.timestamp()));
					}
				});
			}
		}
		LOGGER.debug("End of the application.");
	}


	private Client getKafkaClient(BlockingQueue<String> msgQueue) {
		/** Declare the host you want to connect to, the end-point, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("bitcoin");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a configuration file
		Authentication hosebirdAuth = new OAuth1(cosumerKey, consumerKeySecret, token, tokenSecret);
		final ClientBuilder builder = new ClientBuilder()
		.name("kafka-poc")
		  .hosts(hosebirdHosts)
		  .authentication(hosebirdAuth)
		  .endpoint(hosebirdEndpoint)
		  .processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}
	
	private KafkaProducer<String, String> createKafkaProducer() {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "https://localhost:9092");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		/**
		 * SAFE PRODUCER: Creating an idempotent producer
		 * Kafka <= 0.11
		 * 1. acks = all -> Ensures data is properly replicated before the ack is sent
		 * 2. min.insync.replicas = 2 (broker/topic level setting)
		 * 		- Ensures atleasr 2 ISR brokers are replicated with the data after an ack
		 * 3. retries = Integer.MAX ->  Ensures transient errors are retried indefinitely by producer
		 * 4. max.inflight.requests.per.connection = 1
		 * 		- Ensures ordering is maintained for the messages in case of retries
		 * Kafka >= 0.11
		 * 1. enable.idempotence = true
		 * 2. min.insync.replicas = 2 (broker/topic level)
		 * 		- Keeps the ordering and improved performance
		 * 		- Idempotent producer might have a performance latency
		 */
		properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

		return new KafkaProducer<String, String>(properties);
	}
}
