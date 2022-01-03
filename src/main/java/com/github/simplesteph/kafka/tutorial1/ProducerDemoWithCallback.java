package com.github.simplesteph.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers="127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer= new KafkaProducer<String, String>(properties);


        for(int i=0; i<10; i++) {
            //create a Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world!" + Integer.toString(i));

            //sent data from producer - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //successfully sent or exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata:\n" + "Topic: " + recordMetadata.topic() + "\nPartition: " + recordMetadata.partition() + "\nOffset: " + recordMetadata.offset() + "\nTimeStamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing:", e);
                    }

                }
            });
        }
        //flush data
        producer.flush();

        //flush and close
        producer.close();
    }
}
