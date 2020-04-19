package leason1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumerGroups {
    public static void main(String[] args) {

        //init logger
        Logger logger = LoggerFactory.getLogger(SimpleConsumerGroups.class);

        //consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "first-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //init consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        //subscribe to topic(s)
        kafkaConsumer.subscribe(Collections.singletonList("first-topic"));

        while(true){
            ConsumerRecords<String, String> records =
                    kafkaConsumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String,String> record : records) {
                logger.info("Key - " + record.key() + " value - " + record.value());
                logger.info("Partition - " + record.partition() + " offset - " + record.offset());
            }
        }



    }
}
