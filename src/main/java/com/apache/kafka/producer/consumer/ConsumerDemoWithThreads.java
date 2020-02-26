package com.apache.kafka.producer.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);

      //  Runnable myConsumerThread = new ConsumerThread(bootstrapServers,groupId,topic,latch);

        //Create consumer conf
        //Subscribe consumer to our topics
//        consumer.subscribe(Arrays.asList(topic));

        //poll for new data

    }
    private  ConsumerDemoWithThreads(){

    }
    private   void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
            // create the consumer runnable
            logger.info("Creating the consumer thread");
          Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers,groupId,topic,latch);

          // Start the thread
          Thread thread = new Thread(myConsumerRunnable);
          thread.start();
            // add a shutdown hook
                Runtime.getRuntime().addShutdownHook(new Thread( () -> {
                    logger.info("Caught shutdown hook");
                    ((ConsumerRunnable)myConsumerRunnable).shutdown();
                    try{
                        latch.await();
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                    logger.info("Application has exited");
                }

                ));
          try{
              latch.await();
          }catch (InterruptedException e){
              logger.error("Application got interrupted",e);
          }finally {
              logger.info("Application is closing");
          }
    }

    public class ConsumerRunnable implements  Runnable{
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        public  ConsumerRunnable(String bootstrapServers, String groupId, String topic,CountDownLatch latch){
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            consumer = new KafkaConsumer<String, String>(properties);
        }
        @Override
        public  void run(){
            try{
                while (true){
                    ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String,String> record : records){
                        logger.info("Key : "+record.key()+ " Value : "+record.value());
                        logger.info("Partition : "+record.partition()+ " Offset : "+record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("Received shutdown Signal...");
            }finally {
                consumer.close();
                //tell the main code that we are done with consumer
                latch.countDown();
            }
        }
        public  void shutdown(){
            //The wakeup() method is a special method to interrupt consumer .poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }

}
