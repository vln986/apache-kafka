package com.apache.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    String consumerKey = "7602sIe9GL6ErkQW7dbmgI98L";
    String consumerSecret = "BwK0CANPQ4uzX0IkfOKs5uqo7R6pMClgw8WkZ2fCcoALhPBQ2H";
    String token = "350729647-BJ9bTQgnpLLDrb4dHIrawuoe8x2m82PohjbVPGc1";
    String secret = "3Yj4HdWSQsCdR7GrWtwojTl9dgmtpm0uwkSKLudNC7qhu";
    List<String> terms = Lists.newArrayList("kafka");
    // List<String> terms = Lists.newArrayList("bitcoin","usa","politics","sport","soccer");

    public  TwitterProducer(){

    }
    public static void main(String[] args) {
        new TwitterProducer().run();
    }
    public void run(){
        // Attempts to establish a connection.
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        //create Kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Application");
            logger.info("Stopping client from twitter");
            client.stop();
            logger.info("Stopping producer");
            producer.close();
            logger.info("Done");
        }));

        while (!client.isDone()) {
            String msg = null;
            try{
                msg = msgQueue.poll(5,TimeUnit.SECONDS);
            }catch (InterruptedException e){
                e.printStackTrace();
                client.stop();
            }
           if(msg != null){
               logger.info(msg);
               producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
                   @Override
                   public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                       if(e != null){
                           logger.info("Something went wrong",e);
                       }
                   }
               });
           }
        }
        logger.info("End of Application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
    public KafkaProducer<String,String> createKafkaProducer(){
        String bootstrapServers = "127.0.0.1:9092";

        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5"); // kafka version is >= 1.1 so we can keep this as 5. otherwise use 1

        //hght throughput producer(at the expense of bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));


        //create a producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        return  producer;
    }
}
