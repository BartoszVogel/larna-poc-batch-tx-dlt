package pl.larna.kafka.batch.kafka_batch_demo;

import java.time.Duration;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures graceful shutdown for Kafka listener containers so that the application
 * waits for in-flight record processing to finish before terminating.
 */
@Configuration
class GracefulShutdownConfiguration {

  private static final Logger log = LoggerFactory.getLogger(GracefulShutdownConfiguration.class);

  /**
   * Customize the default Kafka listener container factory to ensure graceful shutdown.
   * We reuse Spring Boot's configurer so all properties from application.yml still apply.
   */
  @Bean
  ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
      ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
      ConsumerFactory<Object, Object> consumerFactory,
      org.springframework.kafka.listener.CommonErrorHandler errorHandler) {
    var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
    // Apply Spring Boot autoconfiguration first
    configurer.configure(factory, consumerFactory);

    // Ensure our global error handler (DefaultErrorHandler) is applied
    factory.setCommonErrorHandler(errorHandler);

    // Ensure graceful shutdown of containers
    factory.getContainerProperties().setStopImmediate(false); // finish in-flight work
    factory.getContainerProperties().setShutdownTimeout(Duration.ofSeconds(30).toMillis());
    return factory;
  }

  @EventListener(ContextClosedEvent.class)
  void onContextClosed(ContextClosedEvent event) {
    log.info("ContextClosedEvent received - initiating graceful shutdown of Kafka listeners.");
  }
}
