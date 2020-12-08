package com.github.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class);

	public static RestHighLevelClient createClient() {
		// https://znlso4jszp:llxxwpxxey@kafka-udemy-tutorial-9493704951.eu-west-1.bonsaisearch.net:443
		String hostName = "kafka-udemy-tutorial-9493704951.eu-west-1.bonsaisearch.net";
		String username = "znlso4jszp";
		String password = "llxxwpxxey";

		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY,
				new UsernamePasswordCredentials(username, password));
		RestClientBuilder builder = RestClient.builder(
				new HttpHost(hostName, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}

	public static void main(String args[]) {
		KafkaConsumer<String, String> consumer = creatKafkaConsumer("twitter");
		RestHighLevelClient client = createClient();
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, String> record: records) {
				IndexRequest request = new IndexRequest(
						"twitter",	// Index
						"tweets"		// Type
						).source(record, XContentType.JSON);
				try {
					IndexResponse response = client.index(request, RequestOptions.DEFAULT);
					String id = response.getId();
					LOGGER.info(id);
					// Close the client graefully
					client.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static KafkaConsumer<String, String> creatKafkaConsumer(String topic) {
		// Just copy from the kafka basics consumer class

		String bootstrapServer = "localhost:9092";
		String groupId = "tweets";
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));

		return consumer;
	}
}
