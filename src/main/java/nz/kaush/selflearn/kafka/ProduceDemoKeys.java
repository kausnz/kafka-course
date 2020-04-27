package nz.kaush.selflearn.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProduceDemoKeys {

    private static Logger logger = LoggerFactory.getLogger(ProduceDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = "http://localhost:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Callback callback = (metaData, e) -> {
            if (e == null) {
                logger.info("Received new metadata. Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                        metaData.topic(), metaData.partition(), metaData.offset(), metaData.timestamp());
            } else {
                logger.error("Error while producing", e);
            }
        };

        for (int i = 0; i < 20; i++) {
            String topic = "topic.one";
            String value = "hello from java, message " + 1;
            String key = "Key_" + i;

            logger.info("Sending message with key {}", key);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            // send data - async
            producer.send(record, callback).get();
        }

        producer.close();
    }
}
