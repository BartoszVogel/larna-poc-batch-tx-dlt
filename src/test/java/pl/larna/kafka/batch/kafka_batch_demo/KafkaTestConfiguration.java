package pl.larna.kafka.batch.kafka_batch_demo;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;

@TestConfiguration
public class KafkaTestConfiguration {

  @Autowired
  KafkaProperties kafkaProperties;

  @Bean
  KafkaConsumer<byte[], byte[]> dltConsumer(AppKafkaProperties appKafkaProperties) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-dlt-consumer-" + UUID.randomUUID());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    String topic = appKafkaProperties.getOutbound().getTransactionRejectedDlt().getTopic();
    KafkaConsumer<byte[], byte[]> dltConsumer = new KafkaConsumer<>(props);
    dltConsumer.subscribe(List.of(topic));
    return dltConsumer;
  }

  @Bean
  KafkaProducer<byte[], byte[]> byteArrayKafkaProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return new KafkaProducer<>(props);
  }

}
