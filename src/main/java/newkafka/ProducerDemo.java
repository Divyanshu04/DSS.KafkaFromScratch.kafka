package newkafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Logger;

public class ProducerDemo {
    public static void main(String[] args) {
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
        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hey Danish Had ur dinner?");
        //send data -asynchronous
        producer.send(record);
        //flush data
        producer.flush();
        //flush and close producer
        producer.close();
    }
}
