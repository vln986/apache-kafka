package com.apache.kafka.producer.consumer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.LocalDateTime;
import java.util.Properties;


public class ProducerDemo {
    public static void main(String[] args) {

        final Logger logger =  LoggerFactory.getLogger(ProducerDemo.class);
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

        System.out.printf("%d - %d - %d : %d : %d : %d : %d", dd,mm,yyyy,hr,min,sec,ns);

        //create a producer record
        final ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","HelloWorld...24 February : "+dd+" - "+mm+" - "+yyyy+" : "+hr+" : "+min+" : "+sec+" : "+ns);

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
        });

        //flush data
        producer.flush();

        //flush and close Producer
        producer.close();
    }
}
