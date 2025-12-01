package pl.larna.kafka.batch.kafka_batch_demo.dlt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.translation.avro.BatchTransactionEvent;
import com.translation.avro.Transaction;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import pl.larna.kafka.batch.kafka_batch_demo.BaseIntegrationTest;
import pl.larna.kafka.batch.kafka_batch_demo.external.DummyService;

class TransactionRejectedListenerIntegrationTest extends BaseIntegrationTest {

  private String inboundTopic;

  @MockitoBean
  private DummyService dummyService;

  @BeforeEach
  void setUp() {
    inboundTopic = appKafkaProperties.getInbound().getTransactionRejected().getTopic();
    doThrow(new RuntimeException("Oops")).when(dummyService)
        .doSomething(argThat(t -> t.getDescription().toLowerCase().contains("error")));
  }

  @Test
  void shouldNotSentToDlt_validTransaction() {
    // given
    Transaction ok = generateTransaction(10.0, "ok");
    Transaction fine = generateTransaction(5.0, "fine");
    BatchTransactionEvent event = buildEvent(
        UUID.randomUUID().toString(),
        List.of(ok, fine)
    );

    // when
    kafkaTemplate.send(inboundTopic, "fake-batch-id-1", event);

    // then
    List<ConsumerRecord<byte[], byte[]>> dlt = pollDlt(Duration.ofSeconds(2));
    assertThat(dlt.isEmpty());
  }

  @Test
  void shouldSentToDlt_invalidTransaction() {
    // given
    Transaction ok = generateTransaction(10.0, "ok");
    Transaction error = generateTransaction(12.0, "contains ERROR text");
    Transaction ok2 = generateTransaction(123.0, "ok 2");
    BatchTransactionEvent event = buildEvent(UUID.randomUUID().toString(), List.of(ok, error, ok2));

    // when
    kafkaTemplate.send(inboundTopic, "fake-batch-id-2", event);

    // then
    waitForDltRecords(error.getTransactionId(), (rec) -> {
      assertThat(rec.key()).isNotNull().asString().isEqualTo(error.getTransactionId());
      assertThat(rec.value()).isNotNull().asString().contains(error.getDescription());
    });

    verify(dummyService, times(3)).doSomething(any(Transaction.class));
  }

  @Test
  void shouldSentToDlt_whenDeserializationFailure() {
    // given
    String key = "bad-key-" + UUID.randomUUID();
    String invalidPayload = "{not-avro}";

    // when
    byteArrayKafkaProducer.send(
        new ProducerRecord<>(inboundTopic, key.getBytes(), invalidPayload.getBytes())
    );

    // then
    waitForDltRecords(key, (rec) -> {
      assertThat(rec.key()).isNotNull().isEqualTo(key.getBytes());
      assertThat(rec.value()).isNotNull().contains(invalidPayload.getBytes());
    });
  }
}
