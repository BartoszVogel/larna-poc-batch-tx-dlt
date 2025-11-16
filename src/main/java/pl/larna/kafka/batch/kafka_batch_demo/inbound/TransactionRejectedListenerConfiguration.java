package pl.larna.kafka.batch.kafka_batch_demo.inbound;


import com.translation.avro.BatchTransactionEvent;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import pl.larna.kafka.batch.kafka_batch_demo.service.TransactionRejectedService;

@Configuration
class TransactionRejectedListenerConfiguration {

  private static final Logger log = LoggerFactory.getLogger(
      TransactionRejectedListenerConfiguration.class);

  private final TransactionRejectedService transactionRejectedService;

  TransactionRejectedListenerConfiguration(TransactionRejectedService transactionRejectedService) {
    this.transactionRejectedService = transactionRejectedService;
  }

  // @DltHandler is not used with DefaultErrorHandler + custom recoverer.
  @KafkaListener(
      idIsGroup = false,
      id = "transaction-rejected-listener",
      groupId = "rejected-transactions-service",
      topics = "${app.kafka.consumer.transaction-rejected.topic}")
  void transactionRejectedEventListener(
      List<ConsumerRecord<String, BatchTransactionEvent>> events,
      Acknowledgment ack) {

    events.stream()
        .map(this::validateTransaction)
        .forEach(transactionRejectedService::onBatchEvent);
    ack.acknowledge();
  }

  // When using ErrorHandlingDeserializer, invalid records are delivered with null value
  // and error details stored in headers. To ensure such records are handled by the
  // DefaultErrorHandler and sent to DLT (instead of reaching business logic),
  // explicitly fail on nulls here.
  private ConsumerRecord<String, BatchTransactionEvent> validateTransaction(
      ConsumerRecord<String, BatchTransactionEvent> event) {
    if (Objects.isNull(event.value())) {
      log.error("Null payload due to deserialization failure");
      throw new SerializationException("Null payload due to deserialization failure");
    }
    return event;
  }
}
