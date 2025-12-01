package pl.larna.kafka.batch.kafka_batch_demo.service;

import com.translation.avro.BatchTransactionEvent;
import com.translation.avro.Transaction;
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

  public TransactionRejectedService(DummyService dummyService, ConsumerRecordRecoverer deadLetterRecoverer) {
    this.dummyService = dummyService;
    this.dlt = deadLetterRecoverer;
  }

  public void onBatchEvent(ConsumerRecord<String, BatchTransactionEvent> record) {
    BatchTransactionEvent event = record.value();
    log.info("\n########### BATCH ID {} ######", event.getBatchId());
    event.getTransactions().forEach(tx -> onTransactionEvent(record, tx));
  }

  void onTransactionEvent(ConsumerRecord<String, BatchTransactionEvent> record,
      Transaction transaction) {
    try {
      dummyService.doSomething(transaction);
    } catch (Exception e) {
      log.error("Error processing transaction id: {}", transaction.getTransactionId());
      String errorMsg = "Error processing transaction id: " + transaction.getTransactionId();
      moveToDlt(record, transaction, errorMsg);
    }
  }

  private void moveToDlt(ConsumerRecord<String, BatchTransactionEvent> record,
      Transaction transaction, String reason) {
    log.info("Moving transaction {} to DLT: {}", transaction.getTransactionId(), reason);
    ConsumerRecord<String, Transaction> invalidTxRecord = getInvalidTxRecord(record, transaction);
    dlt.accept(invalidTxRecord, new ServiceError(reason));
  }

  private ConsumerRecord<String, Transaction> getInvalidTxRecord(
      ConsumerRecord<String, BatchTransactionEvent> record, Transaction transaction) {
    return new ConsumerRecord<>(
        record.topic(),
        record.partition(),
        record.offset(),
        String.valueOf(transaction.getTransactionId()),
        transaction);
  }
}
