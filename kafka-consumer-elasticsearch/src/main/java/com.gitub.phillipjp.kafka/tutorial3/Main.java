package com.gitub.phillipjp.kafka.tutorial3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Main {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Main.class.getName());

        Properties config = loadConfig(logger);

        ElasticsearchClient esClient = new ElasticsearchClient(config);

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create consumer
        logger.info("Creating the consumer thread");
        KafkaConsumer kafkaConsumer = new KafkaConsumer(config, latch, config.getProperty("app.kafka.topic"));

        // create runnable consumer
        Runnable consumerRunnable = new ConsumerRunnable(kafkaConsumer, esClient);


        // start thread
        Thread myThread = new Thread(consumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("Application got interrupted", e);
        }finally {
            logger.info("Application is closing");
        }

        esClient.closeClient();

        logger.info("End of application");
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
