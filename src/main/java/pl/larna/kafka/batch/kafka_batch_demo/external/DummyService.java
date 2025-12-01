package pl.larna.kafka.batch.kafka_batch_demo.external;

import com.translation.avro.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DummyService {

  private static final Logger log = LoggerFactory.getLogger(DummyService.class);

  public void doSomething(Transaction transaction) {
    log.info("transaction           id: {}", transaction.getTransactionId());
    log.info("transaction       amount: {}", transaction.getAmount());
    log.info("transaction  description: {}", transaction.getDescription());
    log.info("----------------------------------------------------------------");
  }

}
