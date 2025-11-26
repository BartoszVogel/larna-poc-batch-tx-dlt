package pl.larna.kafka.batch.kafka_batch_demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.translation.avro.BatchTransactionEvent;
import com.translation.avro.Transaction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
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
  protected KafkaConsumer<byte[], byte[]> dltConsumer;

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

  protected void waitForDltRecords(String key, Consumer<ConsumerRecord<byte[], byte[]>> requirements) {
    await()
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(() -> {
          var record = pollDlt().stream()
              .filter(rec -> Arrays.equals(rec.key(), key.getBytes()))
              .findFirst();
          assertThat(record).isPresent().hasValueSatisfying(requirements);
        });
  }

  protected List<ConsumerRecord<byte[], byte[]>> pollDlt(Duration timeout) {
    var records = dltConsumer.poll(timeout);
    var out = new ArrayList<ConsumerRecord<byte[], byte[]>>();
    for (var rec : records) {
      log.info("pollDlt got: {}", new String(rec.key()));
      out.add(rec);
    }
    return out;
  }

  protected List<ConsumerRecord<byte[], byte[]>> pollDlt() {
    return pollDlt(Duration.ofMillis(200));
  }
}
