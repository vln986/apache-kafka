package com.apache.kafka.elasticsearch;

import com.google.gson.JsonParser;
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



    public static RestHighLevelClient createClient(){
        //  https://oqxlu4p5ph:mnsjljcums@kafka-course-9612309438.eu-west-1.bonsaisearch.net:443
        String hostname = "kafka-course-9612309438.eu-west-1.bonsaisearch.net";
        String username = "oqxlu4p5ph";
        String password = "mnsjljcums";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname,443,"https")).setHttpClientConfigCallback(
                        new RestClientBuilder.HttpClientConfigCallback(){

                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder){
                                return  httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            }
                        });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return  client;
    }

    public static KafkaConsumer<String,String> createConsumer(String topic){

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        //Create consumer config

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false"); // disabling auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(10));
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(10));

        //Create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return  consumer;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();
       // String jsonString = "{ \"foo\": \"bar\" }";

        KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");
        while (true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            logger.info("Received "+records.count()+" records");

            for(ConsumerRecord<String,String> record : records){
                // 2 strategies
                // 1.
               // String id = record.topic()+"_"+record.partition()+"_"+record.offset();
                //2. elastic search
                String id_ = extractIdFromTweet(record.value());

                IndexRequest indexRequest = new IndexRequest("twitter","tweets",id_).source(record.value(), XContentType.JSON);  // passing id as argument to make consumer as idempotent

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                logger.info(id);
                try{
                    Thread.sleep(1000);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
                logger.info("Key : "+record.key()+ " Value : "+record.value());
                logger.info("Partition : "+record.partition()+ " Offset : "+record.offset());
            }
            logger.info("Comming offsets");
            consumer.commitSync();
            logger.info("Offset have been committed");
            Thread.sleep(10);
        }
    }
    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson){
        //gson library
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }
}
