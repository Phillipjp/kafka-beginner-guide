package com.gitub.phillipjp.kafka.tutorial3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Main.class.getName());

        Properties config = loadConfig(logger);

        ElasticsearchClient esClient = new ElasticsearchClient(config);

        String doc = "{ \"foo\": \"bar\"}";

        esClient.addDocToIndex(doc);

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
