package com.apache.kafka.producer.consumer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger =  LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServers = "127.0.0.1:9092";

        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create a producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        LocalDateTime dt = LocalDateTime.now();
        int dd = dt.getDayOfMonth();
        int mm = dt.getMonthValue();
        int yyyy = dt.getYear();

        int hr = dt.getHour();
        int min = dt.getMinute();
        int sec = dt.getSecond();
        int ns = dt.getNano();

        //System.out.printf("%d - %d - %d : %d : %d : %d : %d", dd,mm,yyyy,hr,min,sec,ns);

        for(int i = 0;i<=10;i++){
            String topic = "first_topic";
            String value = "HelloWorld : "+i;
            String key = "id_"+i;

            //create a producer record
            final ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,key,value);

            logger.info("key : "+key);

            //send data - asynchronous

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if(e == null){
                        //executes when the message was successfully sent
                        logger.info("Received new metadata. \n" +
                                "topic : "+recordMetadata.topic()+"\n"+
                                "Partition : "+recordMetadata.partition()+"\n"+
                                "Offset : "+recordMetadata.offset()+"\n"+
                                "Timestamp : "+recordMetadata.timestamp());

                    }else{
                        logger.info("Error while producing : "+e);
                    }
                }
            }).get(); // blocking the send() method to make synchronous - don't use in Production

        }



        //flush data
        producer.flush();

        //flush and close Producer
        producer.close();
    }
}
