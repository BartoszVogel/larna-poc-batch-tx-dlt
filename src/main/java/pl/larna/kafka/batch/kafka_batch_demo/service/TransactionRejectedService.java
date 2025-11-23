package pl.larna.kafka.batch.kafka_batch_demo.service;

import com.translation.avro.BatchTransactionEvent;
import com.translation.avro.Transaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.stereotype.Component;

@Component
public class TransactionRejectedService {

  private static final Logger log = LoggerFactory.getLogger(TransactionRejectedService.class);

  private final ConsumerRecordRecoverer dlt;

  public TransactionRejectedService(ConsumerRecordRecoverer deadLetterRecoverer) {
    this.dlt = deadLetterRecoverer;
  }

  public void onBatchEvent(ConsumerRecord<String, BatchTransactionEvent> record) {
    BatchTransactionEvent event = record.value();
    log.info("\n########### BATCH ID {} ######", event.getBatchId());
    event.getTransactions().forEach(tx -> onTransactionEvent(record, tx));
  }

  void onTransactionEvent(ConsumerRecord<String, BatchTransactionEvent> record,
      Transaction transaction) {
    log.info("transaction           id: {}", transaction.getTransactionId());
    log.info("transaction       amount: {}", transaction.getAmount());
    log.info("transaction  description: {}", transaction.getDescription());
    if (transaction.getDescription().toLowerCase().contains("error")) {
      log.error("Error processing transaction id: {}", transaction.getTransactionId());
      String errorMsg = "Error processing transaction id: " + transaction.getTransactionId();
      moveToDlt(record, transaction, errorMsg);
    }
    log.info("----------------------------------------------------------------");
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
