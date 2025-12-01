package pl.larna.kafka.batch.kafka_batch_demo.inbound;


import com.translation.avro.TransactionEvent;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
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

  @KafkaListener(
      idIsGroup = false,
      id = "transaction-rejected-listener",
      groupId = "rejected-transactions-service",
      topics = "${app.kafka.inbound.transaction-rejected.topic}",
      containerFactory = "kafkaTransactionRejectedListenerContainerFactory"
  )
  void transactionRejectedEventListener(List<ConsumerRecord<String, TransactionEvent>> events,
      Acknowledgment ack) {
    log.info("Received {} transaction rejected events", events.size());
    for (int recordIdx = 0; recordIdx < events.size(); recordIdx++) {
      ConsumerRecord<String, TransactionEvent> event = events.get(recordIdx);
      validateTransaction(event, recordIdx);
      transactionRejectedService.process(event);
      ack.acknowledge(recordIdx);
    }
  }

  // When using ErrorHandlingDeserializer, invalid records are delivered with null value
  // and error details stored in headers. To ensure such records are handled by the
  // DefaultErrorHandler and sent to DLT (instead of reaching business logic),
  // explicitly fail on nulls here.
  private void validateTransaction(
      ConsumerRecord<String, TransactionEvent> event, int recordIdx) {
    if (Objects.isNull(event.value())) {
      log.error("Null payload due to deserialization failure");
      throw new BatchListenerFailedException("Null payload due to deserialization failure",
          recordIdx);
    }
  }
}
