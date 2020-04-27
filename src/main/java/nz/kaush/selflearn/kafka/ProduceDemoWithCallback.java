package nz.kaush.selflearn.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProduceDemoWithCallback {

    private static Logger logger = LoggerFactory.getLogger(ProduceDemoWithCallback.class);

    public static void main(String[] args) {
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
            ProducerRecord<String, String> record = new ProducerRecord<>("topic.one", "hello from java, message " + i);
            // send data - async
            producer.send(record, callback);
        }

        producer.close();
    }
}
