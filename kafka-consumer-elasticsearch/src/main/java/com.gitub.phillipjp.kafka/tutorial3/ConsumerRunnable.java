package com.gitub.phillipjp.kafka.tutorial3;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;

public class ConsumerRunnable implements Runnable {

    private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    KafkaConsumer kafkaConsumer;
    ElasticsearchClient esClient;

    public ConsumerRunnable(KafkaConsumer consumer, ElasticsearchClient esClient) {
        this.kafkaConsumer = consumer;
        this.esClient = esClient;
    }

    @Override
    public void run() {
        // poll for new data
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.getConsumer().poll(Duration.ofMillis(100));

                int recordCount = records.count();

                logger.info("Received " + recordCount + " records");

                BulkRequest bulkRequest = new BulkRequest();
                ArrayList<String> tweetIds = new ArrayList<>();

                for (ConsumerRecord<String, String> record : records) {
                    String tweetId = esClient.getTweetId(record.value());
                    IndexRequest indexRequest = esClient.makeIndexRequest(record.value(), tweetId);
                    bulkRequest.add(indexRequest);
                    tweetIds.add(tweetId);
                }


                if (recordCount > 0) {
                    try {
                        logger.info("Adding " + tweetIds + " to index");
                        BulkResponse bulkItemResponses = esClient.client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    } catch (IOException e) {
                        logger.error("Failed to add " + tweetIds + " to index", e);
                    }

                    logger.info("Committing offsets...");
                    kafkaConsumer.getConsumer().commitSync();
                    logger.info("Offsets committed");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (WakeupException e) {
            logger.info("Received shutdown signal!");
            kafkaConsumer.getConsumer().close();
            //tell main code we're down with the consumer
            kafkaConsumer.getLatch().countDown();
        }
    }

    public void shutdown() {
        // method to interrupt consumer.poll
        //will throw a WakeupException
        kafkaConsumer.getConsumer().wakeup();
    }
}
