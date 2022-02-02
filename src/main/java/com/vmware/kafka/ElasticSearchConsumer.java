package com.vmware.kafka;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static void main(String[] args) throws IOException {
        Logger logger= LoggerFactory.getLogger(ElasticSearchConsumer.class);

        //Connect to ElasticSearch
        RestHighLevelClient client=createClient();

      //  String jsonString="{\"test\":\"Some Value\"}";

        // COnnect to Kafka Consumer

        KafkaConsumer<String,String> consumer=createKafkaConsumer("vmware_twitter_tweets");
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record:records){

                // Insert the data from here to ES
                IndexRequest indexRequest=new IndexRequest(
                        "vmware_tweets",
                        "tweet"
                ).source(record.value(), XContentType.JSON);

                IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
                String id=indexResponse.getId();
                logger.info("ID:"+id);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // Logging
              //  logger.info("Key:"+record.key()+", Value:"+record.value());
               // logger.info("Partition:"+record.partition()+" | Offset:"+record.offset());
            }
        }

        //Gracefully close the client

        //client.close();
    }

    public static RestHighLevelClient createClient(){

        //Setting up the credentials
        String hostname="";
        String username="";
        String password="";

        //Credentials Provider Help
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder= RestClient.builder(
            new HttpHost(hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client=new RestHighLevelClient(builder);
        return client;

    }

    public static KafkaConsumer<String,String> createKafkaConsumer(String topic){
        String bootstrapServer="127.0.0.1:9092";
        String groupId="kafka-es-app";

        // Create the Consumer Configs
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest"); //earliest/latest/ none

        //Create a Consumer
        KafkaConsumer<String, String> consumer=new KafkaConsumer<String, String>(properties);

        //Subscribe Consumer to the topics

//        consumer.subscribe(Collections.singleton("vmware_first_topic"));
        consumer.subscribe(Arrays.asList(topic));
        return  consumer;
    }



}
