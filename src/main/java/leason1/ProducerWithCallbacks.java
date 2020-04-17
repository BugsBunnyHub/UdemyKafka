package leason1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallbacks {
    public static void main(String[] args) {
        Properties properties = new Properties();

        //init the logger
        Logger logger = LoggerFactory.getLogger(ProducerWithCallbacks.class);

        //producer properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //init producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //init record
        ProducerRecord<String, String> record = new ProducerRecord<>("first-topic", "hello world");

        for (int i = 0; i < 50; i++) {
            producer.send(record, (recordMetadata, e) -> {
                //executes every time a record is sent
                if (e == null)
                    logger.info(recordMetadata.toString());
                else logger.error(e.getMessage());
            });
        }
        producer.flush();
        producer.close();
    }
}
