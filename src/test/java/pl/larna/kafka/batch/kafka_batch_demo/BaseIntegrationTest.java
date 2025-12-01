package pl.larna.kafka.batch.kafka_batch_demo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.translation.avro.TransactionEvent;
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
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import pl.larna.kafka.batch.kafka_batch_demo.external.DummyService;
import pl.larna.kafka.batch.kafka_batch_demo.service.Transaction;
import pl.larna.kafka.batch.kafka_batch_demo.service.TransactionMapper;

@ActiveProfiles("test")
@SpringBootTest(classes = {KafkaTestConfiguration.class})
public abstract class BaseIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(BaseIntegrationTest.class);

  @Autowired
  protected AppKafkaProperties appKafkaProperties;

  @Autowired
  protected KafkaTemplate<String, TransactionEvent> kafkaTemplate;

  @Autowired
  protected KafkaProducer<byte[], byte[]> byteArrayKafkaProducer;

  @Autowired
  protected KafkaConsumer<byte[], byte[]> dltConsumer;

  @Autowired
  protected TransactionMapper mapper;

  @MockitoSpyBean
  protected DummyService dummyService;

  protected TransactionEvent buildEvent(Transaction tx) {
    return mapper.transactionToTransactionEvent(tx);
  }

  protected Transaction generateTransaction(double amount, String description) {
    return new Transaction("transactionId-" + UUID.randomUUID(), amount, description);
  }

  protected void waitForDltRecords(String key,
      Consumer<ConsumerRecord<byte[], byte[]>> requirements) {
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

  protected String buildKeyRecord(String key) {
    return key + "-" + UUID.randomUUID();
  }
}
