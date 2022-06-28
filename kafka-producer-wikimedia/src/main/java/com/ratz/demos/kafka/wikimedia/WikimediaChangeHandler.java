package com.ratz.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

  private String topic;
  private KafkaProducer<String, String> kafkaProducer;
  private static final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getName());


  public WikimediaChangeHandler(String topic, KafkaProducer<String, String> kafkaProducer) {
    this.topic = topic;
    this.kafkaProducer = kafkaProducer;
  }


  @Override
  public void onOpen() {

    //nothing here
  }

  @Override
  public void onClosed() {

    kafkaProducer.close();
  }

  @Override
  public void onMessage(String event, MessageEvent messageEvent){

    log.info(messageEvent.getData());

    //async
    kafkaProducer.send(new ProducerRecord<>(topic,messageEvent.getData()));
  }

  @Override
  public void onComment(String comment) {
    //nothing
  }

  @Override
  public void onError(Throwable t) {
    log.error("Error on Stream reading" + t.getStackTrace());
  }
}
