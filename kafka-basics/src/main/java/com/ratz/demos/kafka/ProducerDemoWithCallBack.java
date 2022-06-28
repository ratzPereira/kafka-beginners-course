package com.ratz.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

  private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());

  public static void main(String[] args) {

    //create Producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


    //create producer
    KafkaProducer<String,String> producer = new KafkaProducer<>(properties);


    //producer record
    ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java", "hello");



    //send data - async
    producer.send(producerRecord, (metadata, exception) -> {

      //executes everytime a record is successfully sent or exception is thrown
      if(exception == null){
        logger.info("Got metadata " + "Topic, Partition, Offset, Timestamp  " + "\n"
            + metadata.topic() + "\n" +  metadata.partition() + "\n" +  metadata.offset()+ "\n" + metadata.timestamp());
      } else {
        logger.error("Error producing");
      }
    });



    //flush producer
    producer.flush();


    //close producer
    producer.close();

  }
}
