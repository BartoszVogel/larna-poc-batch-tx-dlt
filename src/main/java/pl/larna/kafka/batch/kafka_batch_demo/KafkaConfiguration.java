package pl.larna.kafka.batch.kafka_batch_demo;

import com.translation.avro.BatchTransactionEvent;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import pl.larna.kafka.batch.kafka_batch_demo.service.ServiceError;

@EnableKafka
@Configuration
class KafkaConfiguration {

  private static final Logger log = LoggerFactory.getLogger(KafkaConfiguration.class);

  @Value("${app.kafka.consumer.transaction-rejected.topic}")
  private String transactionRejectedTopic;

  @Value("${app.kafka.consumer.transaction-rejected.backoff.interval}")
  private long backoffInterval;

  @Value("${app.kafka.consumer.transaction-rejected.backoff.max-attempts}")
  private long maxAttempts;

  @Value("${app.kafka.consumer.transaction-rejected-dlt.topic}")
  private String transactionRejectedDltTopic;

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Bean
  NewTopic transactionRejectedTopic() {
    return TopicBuilder.name(transactionRejectedTopic).build();
  }

  @Bean
  NewTopic transactionRejectedDltTopic() {
    return TopicBuilder.name(transactionRejectedDltTopic).build();
  }

  @Bean
  public KafkaTemplate<String, BatchTransactionEvent> producerKafkaTemplate(
      KafkaProperties kafkaProperties) {
    Map<String, Object> kafkaPropertiesMap = kafkaProperties.buildProducerProperties(null);
    return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(kafkaPropertiesMap));
  }

  // ---------------------------------------------------------------------------
  // DLT producer (raw bytes key and value, no Schema Registry)
  // ---------------------------------------------------------------------------
  @Bean
  public ProducerFactory<byte[], byte[]> dltProducerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    // DO NOT set schema.registry.url here
    return new DefaultKafkaProducerFactory<>(props);
  }

  @Bean
  public KafkaTemplate<byte[], byte[]> dltKafkaTemplate() {
    return new KafkaTemplate<>(dltProducerFactory());
  }

  @Bean
  CommonErrorHandler errorHandler(ConsumerRecordRecoverer deadLetterPublishingRecoverer) {
    FixedBackOff backOff = new FixedBackOff(backoffInterval, maxAttempts);
    DefaultErrorHandler handler = new DefaultErrorHandler(deadLetterPublishingRecoverer, backOff);
    // Optionally, add exceptions that should not be retried using handler.addNotRetryableExceptions(...)
    handler.addNotRetryableExceptions(ServiceError.class);
    return handler;
  }

  @Bean
  ConsumerRecordRecoverer deadLetterPublishingRecoverer(
      KafkaTemplate<byte[], byte[]> dltKafkaTemplate) {
    return new ByteArrayDeadLetterRecoverer(dltKafkaTemplate, (record, ex) -> {
      String dltTopic = record.topic() + ".dlt";
      log.warn("Publishing failed record to DLT topic={} payload={} cause={}",
          dltTopic, record, ex.toString());
      return new TopicPartition(dltTopic, record.partition());
    });
  }
}
