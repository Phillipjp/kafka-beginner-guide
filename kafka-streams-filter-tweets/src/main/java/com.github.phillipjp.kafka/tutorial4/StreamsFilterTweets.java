package com.github.phillipjp.kafka.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;


public class StreamsFilterTweets {

    private static JsonParser jsonParser= new JsonParser();

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());

        Properties config = loadConfig(logger);

        int minimumFollowers = 10000;

        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("app.kafka.bootstrapServer"));
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream(config.getProperty("app.kafka.topic"));
        KStream<String, String> filteredStream =  inputTopic.filter(
                (k, jsonTweet) ->
                        //filter for tweets from user with over 10k followers
                hasMorethanNFollowers(jsonTweet, minimumFollowers)
        );
        filteredStream.to("important_tweets");
        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // start stream
        kafkaStreams.start();
    }

    private static boolean hasMorethanNFollowers(String jsonTweet, int n){
        try {
            return jsonParser.parse(jsonTweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count").getAsInt() > n;
        }
        catch (NullPointerException e){
            return false;
        }
    }


    private static Properties loadConfig(Logger logger) {
        Properties config = new Properties();
        String fileName = new File(".").getAbsolutePath() + "/src/main/resources/app.config";
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException e) {
            logger.error("Can't find config file.", e);
        }
        try {
            config.load(is);
        } catch (IOException e) {
            logger.error("Failed to load config.", e);
        }
        return config;
    }
}
