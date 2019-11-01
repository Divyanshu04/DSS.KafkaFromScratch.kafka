package newkafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups{
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-third1-application";
        String topic = "first_topic";
        //create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);

        //Subscribe consumer to our topics:we will select collection bcoz we will create
        //collection of topic not will pattern
        //consumer.subscribe(Collections.singleton(topic));
        //if we want more topic
        //consumer.subscribe(Arrays.asList("first_topic","Second_topic","...."));
        consumer.subscribe(Arrays.asList(topic));
        //poll for new data
        //:consumer does not not get data untill it ask for data
        while(true){
            //consumer.poll(100);// older one kafka
            //OR
            // consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0
            // if error will come in above line than update it with java 8
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record:records){
                logger.info("key: " + record.key() + " ,Value: " + record.value());
                logger.info("Partition: " + record.partition()+ ",offset:" + record.offset());

            }
        }
    }
}

