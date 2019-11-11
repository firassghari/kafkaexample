package com.firas.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerWithCallback {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(KafkaProduce.class);
        Properties prop = new Properties();
        final String bootstrapServer = "127.0.0.1:9092";


            prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        for (int i=0 ; i<10 ; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("firas", "hello world "+Integer.toString(i));
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("message reçu dans le topic" + recordMetadata.topic());
                    } else {
                        logger.error("problème lors de l'envoie du message", e);
                    }
                }
            });
            System.out.println(i);
        }
            producer.flush();
            producer.close();


    }
}
