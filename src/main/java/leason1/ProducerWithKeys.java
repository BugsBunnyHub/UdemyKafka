package leason1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();

        //init the logger
        Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class);

        //producer config
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //init producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //To see valid results about how kafka distribute records with different keys across the partitions(using murmur)
        // use a topic with 3+ partitions

        for (int i = 0; i < 50; i++) {
            //init record
            String topic = "second-topic";
            String value = "hello world" + i;
            String key = " key_" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (recordMetadata, e) -> {
                //executes every time a record is sent
                if (e == null)
                    logger.info(recordMetadata.toString());
                else logger.error(e.getMessage());
            }).get();//block the .send to make it synchronous, it waits for ack from the producer to send the next record
        }
        producer.flush();
        producer.close();
    }
}
