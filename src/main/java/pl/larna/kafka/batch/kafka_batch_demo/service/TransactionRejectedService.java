package pl.larna.kafka.batch.kafka_batch_demo.service;

import com.translation.avro.TransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.stereotype.Component;
import pl.larna.kafka.batch.kafka_batch_demo.external.DummyService;

@Component
public class TransactionRejectedService {

  private static final Logger log = LoggerFactory.getLogger(TransactionRejectedService.class);

  private final DummyService dummyService;
  private final ConsumerRecordRecoverer dlt;
  private final TransactionMapper mapper;

  public TransactionRejectedService(DummyService dummyService,
      ConsumerRecordRecoverer deadLetterRecoverer, TransactionMapper mapper) {
    this.dummyService = dummyService;
    this.dlt = deadLetterRecoverer;
    this.mapper = mapper;
  }

  public void process(ConsumerRecord<String, TransactionEvent> record) {
    Transaction transaction = mapper.transactionEventToTransaction(record.value());
    onTransactionEvent(record, transaction);
  }

  void onTransactionEvent(ConsumerRecord<String, TransactionEvent> record,
      Transaction transaction) {
    try {
      dummyService.doSomething(transaction);
      log.info("Finished processing transaction id: {}", transaction.transactionId());
    } catch (Exception e) {
      log.error("Error processing transaction id: {}", transaction.transactionId());
      String errorMsg = "Error processing transaction id: " + transaction.transactionId();
      moveToDlt(record, transaction, errorMsg);
    }
  }

  private void moveToDlt(ConsumerRecord<String, TransactionEvent> record,
      Transaction transaction, String reason) {
    log.info("Moving transaction {} to DLT: {}", transaction.transactionId(), reason);
    ConsumerRecord<String, Transaction> invalidTxRecord = getInvalidTxRecord(record, transaction);
    dlt.accept(invalidTxRecord, new RuntimeException(reason));
  }

  private ConsumerRecord<String, Transaction> getInvalidTxRecord(
      ConsumerRecord<String, TransactionEvent> record, Transaction transaction) {
    return new ConsumerRecord<>(
        record.topic(),
        record.partition(),
        record.offset(),
        String.valueOf(transaction.transactionId()),
        transaction);
  }
}
