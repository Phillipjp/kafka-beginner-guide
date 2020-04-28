package com.github.phillipjp.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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
            String topic = "first_topic";
            String value = "Hello World " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            logger.info("Key: " + key);
            // id_0 -> partition 1
            // id_1 -> partition 0
            // id_2 -> partition 2
            // id_3 -> partition 0
            // id_4 -> partition 2
            // id_5 -> partition 2
            // id_6 -> partition 0
            // id_7 -> partition 2
            // id_8 -> partition 1
            // id_9 -> partition 2


            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
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
            }).get();
        }
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();

    }
}
