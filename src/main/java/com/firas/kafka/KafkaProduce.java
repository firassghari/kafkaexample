package com.firas.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProduce {
    public static void main(String[] args) {
        Properties prop = new Properties();
        final String bootstrapServer = "127.0.0.1:9092";
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String ,String>(prop);
        ProducerRecord<String,String > record = new ProducerRecord<String, String>("firas","hello world ");
        producer.send(record);
        producer.flush();
        producer.close();

    
    }
}
