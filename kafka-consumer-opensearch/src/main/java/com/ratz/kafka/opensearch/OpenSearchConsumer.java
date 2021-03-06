package com.ratz.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {


  public static RestHighLevelClient createOpenSearchClient() {


    String connString = "http://localhost:9200";


    // we build a URI from the connection string
    RestHighLevelClient restHighLevelClient;
    URI connUri = URI.create(connString);
    // extract login information if it exists
    String userInfo = connUri.getUserInfo();

    if (userInfo == null) {
      // REST client without security
      restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

    } else {
      // REST client with security
      String[] auth = userInfo.split(":");

      CredentialsProvider cp = new BasicCredentialsProvider();
      cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

      restHighLevelClient = new RestHighLevelClient(
          RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
              .setHttpClientConfigCallback(
                  httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                      .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


    }

    return restHighLevelClient;
  }


  private static KafkaConsumer<String, String> createKafkaConsumer() {

    String boostrapServers = "127.0.0.1:9092";
    String groupId = "consumer-opensearch-demo";

    // create consumer configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // not going commit automatically

    // create consumer
    return new KafkaConsumer<>(properties);
  }

  private static String extractId(String value) {

    //gson library
    return JsonParser.parseString(value)
        .getAsJsonObject()
        .get("meta")
        .getAsJsonObject()
        .get("id")
        .getAsString();
  }




  public static void main(String[] args) throws IOException {

    Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());


    //first create an OpenSearch Client
    RestHighLevelClient openSearchClient = createOpenSearchClient();


    // create our kafka client
    KafkaConsumer<String,String> consumer = createKafkaConsumer();


    //we need to create the index on OpenSearch if it doesn't exist already
    //we use try block, so no matter what, the client will be closed
    try(openSearchClient; consumer) {

      boolean indexExist = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

      if (!indexExist){
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
        openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);

        log.info("Wikimedia index has been created!");
      }

      log.info("The Wikimedia index already exist");


      //subscribe to topic
      consumer.subscribe(Collections.singleton("wikimedia.recentchange"));



      while (true){

        //we create records
        ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(3000));
        int recordCount = records.count();

        log.info("Received, {} records!", recordCount);

        BulkRequest bulkRequest = new BulkRequest();

        for (ConsumerRecord<String,String> record : records){

          //make idempotent
          //strategy 1
          //define an id using kafka record coordinates
          //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

          try{

            //strategy 2
            //we extract the ID from the JSON value
            String id = extractId(record.value());

            //Send the record in the open search
            IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);

            bulkRequest.add(indexRequest);
            //IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
            //log.info("Inserted one document into OpenSearch with the ID: {}", response.getId());
          } catch (Exception e) {
            log.error(e.getMessage());
          }
        }

        if(bulkRequest.numberOfActions() > 0) {

          BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
          log.info("Inserted {} records as an bulk", bulkResponse.getItems().length);

          try {
            Thread.sleep(1000);

          } catch (Exception e){

          }

          //commit offsets after batch is consumed
          consumer.commitSync();
          log.info("Offsets have been committed!");
        }


      }
    }


    //close things

  }


}
