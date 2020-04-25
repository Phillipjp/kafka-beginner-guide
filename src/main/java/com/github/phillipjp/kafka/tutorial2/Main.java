package com.github.phillipjp.kafka.tutorial2;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Properties;

public class Main {


    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Main.class.getName());

        Properties config = loadConfig(logger);

        System.out.println(config.getProperty("app.twitter.accessToken"));

        // Create Twitter client
        List<String> terms = Lists.newArrayList("corona", "virus", "trump");
        TwitterClient twitterClient = new TwitterClient(config, terms);

        // Create Kafka Producer
        KafkaProducer producer = new KafkaProducer(config);

        // Send tweets via Producer
        producer.run(twitterClient, "twitter_topic");


        logger.info("End of application");
    }

    private static Properties loadConfig(Logger logger){
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
