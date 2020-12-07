package com.github.kafka;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
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
		String jsonString = "{\"foo\": \"bar\"}";
		RestHighLevelClient client = createClient();
		IndexRequest request = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);
		try {
			IndexResponse response = client.index(request, RequestOptions.DEFAULT);
			String id = response.getId();
			LOGGER.info(id);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
