package com.gitub.phillipjp.kafka.tutorial3;

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

import java.io.IOException;
import java.util.Properties;


public class ElasticsearchClient {

    Logger logger = LoggerFactory.getLogger(ElasticsearchClient.class.getName());

    RestHighLevelClient client;

    public ElasticsearchClient(Properties config){

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(config.getProperty("app.elasticsearch.username"),
                config.getProperty("app.elasticsearch.password")));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(config.getProperty("app.elasticsearch.hostname"), 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        this.client = new RestHighLevelClient(builder);

    }

    public void addDocToIndex(String doc){

        IndexRequest indexRequest = new IndexRequest("twitter", "tweets")
                .source(doc, XContentType.JSON);

        try {
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            logger.info("Added " + indexResponse.getId() + " to index");

        } catch (IOException e) {
            logger.error("Failed to add doc\n" + doc + "\nto index", e);
        }

    }

    public void closeClient(){
        try {
            logger.info("Closing Elasticsearch Client");
            client.close();
        } catch (IOException e) {
            logger.error("Error while closing Elasticsearch client", e);
        }
    }


}
