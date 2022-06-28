package com.ratz.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

  private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

  public static void main(String[] args) {

    //create Producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


    //create producer
    KafkaProducer<String,String> producer = new KafkaProducer<>(properties);


    for (int i = 0; i < 10; i++) {

      String topic = "demo_java";
      String value = "Hello its me, the number " + i;
      String key = "id_" + i;

      //producer record
      ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic, key, value);



      //send data - async
      producer.send(producerRecord, (metadata, exception) -> {

        //executes everytime a record is successfully sent or exception is thrown
        if(exception == null){
          logger.info("Got metadata " + "Topic, Partition, Offset, Timestamp, Key  " + "\n"
              + metadata.topic() + "\n" +  metadata.partition() + "\n" +  metadata.offset()+ "\n" + metadata.timestamp() + "\n" + producerRecord.key());
        } else {
          logger.error("Error producing");
        }
      });
    }

    //flush producer
    producer.flush();


    //close producer
    producer.close();

  }
}
