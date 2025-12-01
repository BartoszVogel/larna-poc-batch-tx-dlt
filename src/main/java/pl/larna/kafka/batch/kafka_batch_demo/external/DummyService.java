package pl.larna.kafka.batch.kafka_batch_demo.external;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import pl.larna.kafka.batch.kafka_batch_demo.service.Transaction;

@Component
public class DummyService {

  private static final Logger log = LoggerFactory.getLogger(DummyService.class);

  public void doSomething(Transaction transaction) {
    log.info("----------------------------------------------------------------");
    log.info("transaction           id: {}", transaction.transactionId());
    log.info("transaction       amount: {}", transaction.amount());
    log.info("transaction  description: {}", transaction.description());
    log.info("----------------------------------------------------------------");
  }

}
