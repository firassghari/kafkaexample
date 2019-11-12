package com.firas.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.font.TrueTypeFont;
import sun.rmi.runtime.Log;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(KafkaProduce.class);
        Properties prop = new Properties();
        final String bootstrapServer = "127.0.0.1:9092";
        String groupid = "firas application";
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupid);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String,String > consumer = new KafkaConsumer<String, String>(prop);
        consumer.subscribe(Arrays.asList("firas"));
        while (true)
        {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for ( ConsumerRecord<String,String> record : records)
            {
                logger.info("key="+record.key()+" valuer ="+record.value()+" topic ="+record.topic());
            }
        }




    }

}
