package com.atguigu.utils;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyKafkaSender {
    public static KafkaProducer kafkaProducer = null;

    public static void createKafkaProducer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<String,String>(properties);
    }

    public static void send(String kafkaTopicOrder, String msg) {
        if (kafkaProducer == null){
            createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord(kafkaTopicOrder,msg));
    }
}
