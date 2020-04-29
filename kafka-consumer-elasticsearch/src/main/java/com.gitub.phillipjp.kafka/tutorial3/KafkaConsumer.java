package com.gitub.phillipjp.kafka.tutorial3;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaConsumer{

    private Logger logger = LoggerFactory.getLogger(KafkaConsumer.class.getName());

    private CountDownLatch latch;
    private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer;
    String topic;


    public KafkaConsumer(Properties config, CountDownLatch latch, String topic){
        this.latch = latch;

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("app.kafka.bootstrapServer"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, config.getProperty("app.kafka.consumer.groupId"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        this.topic = topic;

        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));

    }

    public org.apache.kafka.clients.consumer.KafkaConsumer<String, String> getConsumer(){
        return consumer;
    }

    public CountDownLatch getLatch(){
        return latch;
    }

}
