package pl.larna.kafka.batch.kafka_batch_demo;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.with;

import com.translation.avro.BatchTransactionEvent;
import com.translation.avro.Transaction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
@SpringBootTest(classes = {KafkaTestConfiguration.class})
public abstract class BaseIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(BaseIntegrationTest.class);

  @Autowired
  protected AppKafkaProperties appKafkaProperties;

  @Autowired
  protected KafkaTemplate<String, BatchTransactionEvent> kafkaTemplate;

  @Autowired
  protected KafkaProducer<byte[], byte[]> byteArrayKafkaProducer;

  @Autowired
  KafkaConsumer<byte[], byte[]> dltConsumer;

  protected BatchTransactionEvent buildEvent(String batchId, List<Transaction> txs) {
    return BatchTransactionEvent.newBuilder()
        .setBatchId(batchId)
        .setTransactions(txs)
        .build();
  }

  protected Transaction generateTransaction(double amount, String description) {
    return Transaction.newBuilder()
        .setTransactionId(UUID.randomUUID().toString())
        .setAmount(amount)
        .setDescription(description)
        .build();
  }

  protected void drainDlt() {
    pollDlt(Duration.ofSeconds(1));
  }

  protected List<ConsumerRecord<byte[], byte[]>> waitForDltRecords(int expected) {
    List<ConsumerRecord<byte[], byte[]>> acc = new ArrayList<>();
    with().pollDelay(2, SECONDS).await().atMost(5, SECONDS)
        .untilAsserted(() -> {
          List<ConsumerRecord<byte[], byte[]>> dlt = pollDlt(Duration.ofMillis(500));
          log.info("waitForDltRecords got: {} records", dlt.size());
          assertThat(dlt.size()).isGreaterThanOrEqualTo(expected);
          acc.addAll(dlt);
        });
    return acc.stream().limit(expected).toList();
  }

/*  protected Optional<ConsumerRecord<byte[], byte[]>> waitForDltRecords(String key) {
    AtomicReference<Optional<ConsumerRecord<byte[], byte[]>>> expected = new AtomicReference<>();
    await().atMost(5, SECONDS).untilAsserted(() -> {
      List<ConsumerRecord<byte[], byte[]>> dlt = pollDlt(Duration.ofMillis(500));
      log.info("waitForDltRecords got: {} records", dlt.size());
      Optional<ConsumerRecord<byte[], byte[]>> record = dlt.stream()
          .filter(rec -> new String(rec.key()).equals(key))
          .findFirst();
      assertThat(record).isPresent();
      expected.set(record);
    });
    return expected.get();
  }*/

  protected List<ConsumerRecord<byte[], byte[]>> pollDlt(Duration timeout) {
    var records = dltConsumer.poll(timeout);
    var out = new ArrayList<ConsumerRecord<byte[], byte[]>>();
    for (var rec : records) {
      log.info("pollDlt got: {}", new String(rec.key()));
      out.add(rec);
    }
    return out;
  }
}
