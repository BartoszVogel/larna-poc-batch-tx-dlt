package pl.larna.kafka.batch.kafka_batch_demo;

import com.translation.avro.TransactionEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.FixedBackOff;
import pl.larna.kafka.batch.kafka_batch_demo.AppKafkaProperties.BackOff;

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
    return TopicBuilder.name(
            appKafkaProperties.getOutbound().getTransactionRejectedDlt().getTopic())
        .build();
  }

  public ConsumerFactory<String, TransactionEvent> transactionRejectedConsumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
    props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
    props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
    // Disable auto-commit to allow manual offset control
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    // Set the maximum number of records to be returned in a single poll
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10); // Tune based on throughput needs
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 200);
    props.put("schema.registry.url", appKafkaProperties.getSchemaRegistryUrl());
    props.put("specific.avro.reader", true);
    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> kafkaTransactionRejectedListenerContainerFactory(
      CommonErrorHandler errorHandler) {
    ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(transactionRejectedConsumerFactory());

    // Enable batch listener mode
    factory.setBatchListener(true);

    // Set concurrency (number of consumer threads); 1 per partition
    factory.setConcurrency(1); // Increase only if a topic has multiple partitions

    // Set acknowledgment mode to manual
    factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);

    // Register common error handler
    factory.setCommonErrorHandler(errorHandler);

    // Ensure a graceful shutdown of containers
    factory.getContainerProperties().setStopImmediate(false); // finish in-flight work
    factory.getContainerProperties().setShutdownTimeout(Duration.ofSeconds(20).toMillis());

    return factory;
  }

  // ---------------------------------------------------------------------------
  // Producer (string key and BatchTransactionEvent value, with Schema Registry)
  // ---------------------------------------------------------------------------
  @Bean
  public KafkaTemplate<String, TransactionEvent> producerKafkaTemplate(
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
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-dlt-producer");
    return new DefaultKafkaProducerFactory<>(props);
  }

  @Bean
  public KafkaTemplate<byte[], byte[]> dltKafkaTemplate() {
    dltProducerFactory().createProducer().close();
    return new KafkaTemplate<>(dltProducerFactory());
  }

  @Bean
  CommonErrorHandler errorHandler(ConsumerRecordRecoverer deadLetterPublishingRecoverer) {
    BackOff backOffProps = appKafkaProperties.getBackOff();
    FixedBackOff backOff = new FixedBackOff(backOffProps.getInterval(),
        backOffProps.getMaxAttempts());
    return new DefaultErrorHandler(deadLetterPublishingRecoverer, backOff);
  }

  @Bean
  ConsumerRecordRecoverer deadLetterPublishingRecoverer(
      KafkaTemplate<byte[], byte[]> dltKafkaTemplate) {
    return new ByteArrayDeadLetterRecoverer(dltKafkaTemplate, (record, ex) -> {
      String dltTopic = record.topic() + "-dlt";
      log.warn("Publishing failed record to DLT topic={} payload={} cause={}",
          dltTopic, record, ex.toString());
      return new TopicPartition(dltTopic, 0);
    });
  }
}
