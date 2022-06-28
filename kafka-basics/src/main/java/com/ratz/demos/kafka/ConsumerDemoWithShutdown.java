package com.ratz.demos.kafka;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoWithShutdown {

  private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

  public static void main(String[] args) {

    logger.info("Im a Kafka Consumer!");

    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "MyGroupIds-second";
    String topic = "demo_java";

    //creating consumer properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


    // create consumer
    KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);


    //get reference to the current thread
    final Thread mainThread = Thread.currentThread();


    //adding the shutdownhook
    Runtime.getRuntime().addShutdownHook(new Thread(){
      public void run(){
        logger.info("Detected shutdown, lets exit by calling consumer.wakeup()...");
        consumer.wakeup();

        //join the main thread
        try {
          mainThread.join();
        } catch (InterruptedException e){
          e.printStackTrace();
        }
      }
    });

    try{
      // subscribe consumer to topic
      consumer.subscribe(Arrays.asList(topic));


      // poll for new data
      while (true){

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

        for ( ConsumerRecord<String,String> record: records){

          logger.info("Key: " + record.key());
          logger.info("Value: " + record.value());
          logger.info("Partition: " + record.partition());
          logger.info("Offset: " + record.offset());
        }
      }
    } catch (WakeupException e) {
      logger.info("WakeUp Exception!!");
      //ignore because this is an expected exception when closing consumer

    } catch (Exception e){
      logger.error("ERROR!!");

    } finally {
      consumer.close(); //also commit the offsets
      logger.info("Gracefully closed");
    }
  }
}
