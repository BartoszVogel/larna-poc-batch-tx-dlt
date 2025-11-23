package pl.larna.kafka.batch.kafka_batch_demo.dlt;

import static org.assertj.core.api.Assertions.assertThat;

import com.translation.avro.BatchTransactionEvent;
import com.translation.avro.Transaction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.larna.kafka.batch.kafka_batch_demo.BaseIntegrationTest;

class TransactionRejectedListenerIntegrationTest extends BaseIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(
      TransactionRejectedListenerIntegrationTest.class);

  private String inboundTopic;

  @BeforeEach
  void setUp() {
    drainDlt();
    inboundTopic = appKafkaProperties.getInbound().getTransactionRejected().getTopic();
  }

  @Test
  void shouldNotSentToDlt_validTransaction() {
    //given
    Transaction ok = generateTransaction(10.0, "ok");
    Transaction fine = generateTransaction(5.0, "fine");
    BatchTransactionEvent event = buildEvent(
        UUID.randomUUID().toString(),
        List.of(ok, fine)
    );

    //when
    kafkaTemplate.send(inboundTopic, "fake-batch-id-1", event);

    //then
    List<ConsumerRecord<byte[], byte[]>> dlt = pollDlt(Duration.ofSeconds(2));
    assertThat(dlt.isEmpty());
  }

  @Test
  void shouldSentToDlt_invalidTransaction() {
    // given
    Transaction ok = generateTransaction(10.0, "ok");
    Transaction error = generateTransaction(12.0, "contains ERROR text");
    BatchTransactionEvent event = buildEvent(UUID.randomUUID().toString(), List.of(ok, error));

    // when
    kafkaTemplate.send(inboundTopic, "fake-batch-id-2", event);

    // then
    var dlt = waitForDltRecords(1);

    var rec = dlt.getFirst();
    assertThat(rec.key()).isNotNull().asString().isEqualTo(error.getTransactionId());
    assertThat(rec.value()).isNotNull().asString().contains(error.getDescription());
  }

  @Disabled("disabled until we find a way to make it work")
  @Test
  //@RepeatedTest(3)
  void shouldSentToDlt_whenDeserializationFailure() throws InterruptedException {
    // given
    String key = "bad-key-" + UUID.randomUUID();
    String invalidPayload = "{not-avro}";

    // when
    byteArrayKafkaProducer.send(
        new ProducerRecord<>(inboundTopic, key.getBytes(), invalidPayload.getBytes())
    );

    Thread.sleep(Duration.ofSeconds(2));

    // then
    List<ConsumerRecord<byte[], byte[]>> dlt = waitForDltRecords(1);

    ConsumerRecord<byte[], byte[]> rec = dlt.getFirst();
    assertThat(rec.key()).isNotNull().isEqualTo(key.getBytes(StandardCharsets.UTF_8));
    assertThat(rec.value()).isNotNull().contains(invalidPayload.getBytes(StandardCharsets.UTF_8));
  }
}
