package newkafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
//import java.util.logging.Logger;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrapServers = "127.0.0.1:9092";
        //create Producer properties
        Properties properties = new Properties();
        // properties is a object , so we have to provide all property but we are not
        //sure about which property we are needed so we need to check :https://kafka.apache.org/documentation/#producerconfigs
        //first property
        // properties.setProperty("bootstrap.server","127.0.0.1:9092");
        //OR insteed of using of hardcoding value we can use below one
        //properties.setProperty("bootstrap.server",bootstrapServers);
        //new method means no hardcoded
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Second property(what type of value we are going to provide we have to pass in below)
        // properties.setProperty("key.serializer","");
        //OR
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // properties.setProperty("value.serializer","");
        //OR
        //properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//********************************************
        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for(int i =0; i<10; i++){
        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hey Danish Had ur dinner?");
        //send data -asynchronous
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              // onCompletion > execute every time a record is successfully sent or an
              // exception is thrown
               if(e == null){
                   //the record was successfully sent
                   //we will use functionality of recordMetadata, so far this thing we will create logger
                   logger.info("Received new metadata. \n" +
                           "Topic: " + recordMetadata.topic() + "\n" +
                           "Partition: " +recordMetadata.partition() + "\n"+
                           "offset: " + recordMetadata.offset() + "\n"+
                           "Timestamp: " + recordMetadata.timestamp());
               }else{
                   logger.error("Error while producing",e);

               }
            }
        });};
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
