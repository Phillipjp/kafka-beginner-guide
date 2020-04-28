package com.github.phillipjp.kafka.tutorial2;

import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class KafkaProducer {

    org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
    Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    public KafkaProducer(Properties config){

        // create producer properties
        String bootstrapSevers = config.getProperty("app.kafka.bootstrapServer");
        String keySerializer = StringSerializer.class.getName();
        String valueSerializer = StringSerializer.class.getName();

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapSevers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

        // create a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,  config.getProperty("app.kafka.producer.idempotence"));
        // not necessary with enable idempotence equal to true but set just to be explicit
        properties.setProperty(ProducerConfig.ACKS_CONFIG, config.getProperty("app.kafka.producer.acks"));
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, config.getProperty("app.kafka.producer.request.per.connection"));

        // high throughput producer (at cost of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.getProperty("app.kafka.producer.compression"));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, config.getProperty("app.kafka.producer.linger"));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, config.getProperty("app.kafka.producer.batch"));


        // create producer
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);

    }

    public void run(TwitterClient twitterClient,  String topic){
        Client client = twitterClient.getHosebirdClient();
        BlockingQueue<String> msgQueue = twitterClient.getMsgQueue();
        client.connect();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
           logger.info("Stopping application...");
           logger.info("Shutting down twitter client...");
           client.stop();
           logger.info("Shutting down Kafka producer...");
           producer.close();
           logger.info("Application shutdown!");
        }));

        int keyIndex = 0;

        while (!client.isDone()) {

            String key = "id_" + Integer.toString(keyIndex);
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            }catch (InterruptedException e){
                logger.error("Failed for key: " + key + "\nmsgQueue poll interrupted", e);
                client.stop();
            }
            if(msg != null){

                logger.info("Key: " + key);
                final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, msg);
                //send data -asynchronous
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes every time a record is successfully sent ort an exception is thrown
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
            keyIndex++;
        }

        producer.close();
    }

}
