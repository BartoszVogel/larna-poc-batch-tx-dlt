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
import pl.larna.kafka.batch.kafka_batch_demo.AppKafkaProperties.BackOff;
import pl.larna.kafka.batch.kafka_batch_demo.service.ServiceError;

@EnableKafka
@Configuration
class KafkaConfiguration {

  private static final Logger log = LoggerFactory.getLogger(KafkaConfiguration.class);

  private final KafkaProperties kafkaProperties;
  private final AppKafkaProperties appKafkaProperties;

  KafkaConfiguration(KafkaProperties kafkaProperties, AppKafkaProperties appKafkaProperties) {
    this.kafkaProperties = kafkaProperties;
    this.appKafkaProperties = appKafkaProperties;
  }

  @Bean
  NewTopic transactionRejectedTopic() {
    return TopicBuilder.name(appKafkaProperties.getInbound().getTransactionRejected().getTopic())
        .build();
  }

  @Bean
  NewTopic transactionRejectedDltTopic() {
    return TopicBuilder.name(appKafkaProperties.getOutbound().getTransactionRejectedDlt().getTopic())
        .build();
  }

  // ---------------------------------------------------------------------------
  // Producer (string key and BatchTransactionEvent value, with Schema Registry)
  // ---------------------------------------------------------------------------
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
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka_batch_demo-dlt-producer");
    // DO NOT set schema.registry.url here
    return new DefaultKafkaProducerFactory<>(props);
  }

  @Bean
  public KafkaTemplate<byte[], byte[]> dltKafkaTemplate() {
    dltProducerFactory().createProducer().close();
    return new KafkaTemplate<>(dltProducerFactory());
  }

  @Bean
  CommonErrorHandler errorHandler(ConsumerRecordRecoverer deadLetterPublishingRecoverer) {
    BackOff backOffProps = appKafkaProperties.getInbound().getTransactionRejected().getBackOff();
    FixedBackOff backOff = new FixedBackOff(backOffProps.getInterval(), backOffProps.getMaxAttempts());
    DefaultErrorHandler handler = new DefaultErrorHandler(deadLetterPublishingRecoverer, backOff);
    // Optionally, add exceptions that should not be retried using handler.addNotRetryableExceptions(...)
    handler.addNotRetryableExceptions(ServiceError.class);
    return handler;
  }

  @Bean
  ConsumerRecordRecoverer deadLetterPublishingRecoverer(
      KafkaTemplate<byte[], byte[]> dltKafkaTemplate) {
    return new ByteArrayDeadLetterRecoverer(dltKafkaTemplate, (record, ex) -> {
      String dltTopic = appKafkaProperties.getOutbound().getTransactionRejectedDlt().getTopic();
      log.warn("Publishing failed record to DLT topic={} payload={} cause={}",
          dltTopic, record, ex.toString());
      // Use partition 0 to avoid mismatches when the source partition doesn't exist on the DLT topic
      return new TopicPartition(dltTopic, 0);
    });
  }
}
