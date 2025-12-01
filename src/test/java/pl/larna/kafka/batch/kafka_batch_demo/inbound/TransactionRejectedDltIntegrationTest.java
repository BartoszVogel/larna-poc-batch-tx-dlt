package pl.larna.kafka.batch.kafka_batch_demo.inbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import pl.larna.kafka.batch.kafka_batch_demo.BaseIntegrationTest;
import pl.larna.kafka.batch.kafka_batch_demo.service.Transaction;

class TransactionRejectedDltIntegrationTest extends BaseIntegrationTest {

  private String inboundTopic;

  @Autowired
  private KafkaListenerEndpointRegistry registry;

  @BeforeEach
  void setUp() {
    inboundTopic = appKafkaProperties.getInbound().getTransactionRejected().getTopic();
    doThrow(new RuntimeException("Oops")).when(dummyService)
        .doSomething(argThat(t -> t.description().toLowerCase().contains("error")));
  }

  @Test
  void shouldSentToDlt_invalidTransaction() {
    // given
    Transaction ok = generateTransaction(10.0, "ok");
    Transaction error = generateTransaction(12.0, "contains ERROR text");
    Transaction ok2 = generateTransaction(123.0, "ok 2");

    // when
    kafkaTemplate.send(inboundTopic, buildKeyRecord("fake-batch-id-1"), buildEvent(ok));
    kafkaTemplate.send(inboundTopic, buildKeyRecord("fake-batch-id-2"), buildEvent(error));
    kafkaTemplate.send(inboundTopic, buildKeyRecord("fake-batch-id-3"), buildEvent(ok2));

    // then
    waitForDltRecords(error.transactionId(), (rec) -> {
      assertThat(rec.key()).isNotNull().asString().isEqualTo(error.transactionId());
      assertThat(rec.value()).isNotNull().asString().contains(error.description());
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
    verifyNoInteractions(dummyService);
  }

  @Test
  void shouldSentToDlt_whenOneOfRecordDeserializationFailure() {
    // given
    String key = "bad-key-id-" + UUID.randomUUID();
    String invalidPayload = "{not-avro}";
    Transaction ok = generateTransaction(10.0, "ok");
    Transaction ok2 = generateTransaction(123.0, "ok 2");
    registry.getListenerContainer("transaction-rejected-listener").stop();

    // when
    kafkaTemplate.send(inboundTopic, buildKeyRecord("good-key-id-1"), buildEvent(ok));
    byteArrayKafkaProducer.send(
        new ProducerRecord<>(inboundTopic, key.getBytes(), invalidPayload.getBytes())
    );
    kafkaTemplate.send(inboundTopic, buildKeyRecord("good-key-id-3"), buildEvent(ok2));

    registry.getListenerContainer("transaction-rejected-listener").start();

    // then
    waitForDltRecords(key, (rec) -> {
      assertThat(rec.key()).isNotNull().isEqualTo(key.getBytes());
      assertThat(rec.value()).isNotNull().contains(invalidPayload.getBytes());
    });

    await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
        verify(dummyService, times(2)).doSomething(any(Transaction.class))
    );
  }
}
