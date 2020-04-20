package com.github.phillipjp.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

        //create producer properties
        String bootstrapSevers = "127.0.0.1:9092";
        String keySerializer = StringSerializer.class.getName();
        String valueSerializer = StringSerializer.class.getName();

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapSevers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i =0; i <10; i++ ) {
            //create producer record
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World " + Integer.toString(i));
            //send data -asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes evertime a record is successfully sent ort an exception is thrown
                    if (e == null) {
                        // record sent successfully sent
                        logger.info("Recieved new meta data. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing.", e);

                    }
                }
            });
        }
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();

    }
}
