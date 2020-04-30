package com.gitub.phillipjp.kafka.tutorial3;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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

    private static JsonParser jsonParser= new JsonParser();

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

        String tweetId = getTweetId(doc);

        // only get the text so elasticsearch doesn't fill up too quickly
        String tweetText = "{ \"text\": " + jsonParser.parse(doc).getAsJsonObject().get("text").toString()  + "}";

        IndexRequest indexRequest = new IndexRequest("twitter", "tweets",  tweetId)
                .source(tweetText, XContentType.JSON);

        try {
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            logger.info("Added " + tweetId + " to index");
            logger.info(indexResponse.getId());

        } catch (IOException e) {
            logger.error("Failed to add doc\n" + doc + "\nto index", e);
        }

    }

    public IndexRequest makeIndexRequest(String doc, String tweetId){

        // only get the the required fields so elasticsearch doesn't fill up too quickly
//        JsonObject tweetData = jsonParser.parse(doc).getAsJsonObject();
//        JsonObject user = tweetData.get("user").getAsJsonObject();
//        String tweetText = "{ \"text\": " + tweetData.get("text").toString() + "," +
//                " \"user\": {" +
//                " \"id_str\": " + user.get("id_str").toString() + "," +
//                " \"followers_count\": " + user.get("followers_count").toString() + "} }";

        return new IndexRequest("twitter", "tweets",  tweetId)
                .source(doc, XContentType.JSON);
    }

    public String getTweetId(String tweetJson){

        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
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
