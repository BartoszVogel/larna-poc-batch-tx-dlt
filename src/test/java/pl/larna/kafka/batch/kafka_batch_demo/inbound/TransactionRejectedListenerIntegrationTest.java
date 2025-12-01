package pl.larna.kafka.batch.kafka_batch_demo.inbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.larna.kafka.batch.kafka_batch_demo.BaseIntegrationTest;
import pl.larna.kafka.batch.kafka_batch_demo.service.Transaction;

class TransactionRejectedListenerIntegrationTest extends BaseIntegrationTest {

  private String inboundTopic;

  @BeforeEach
  void setUp() {
    inboundTopic = appKafkaProperties.getInbound().getTransactionRejected().getTopic();
  }

  @Test
  void shouldProcessAllTransactions() {
    // given
    Transaction ok = generateTransaction(10.0, "ok");
    Transaction fine = generateTransaction(5.0, "fine");

    // when
    kafkaTemplate.send(inboundTopic, buildKeyRecord("fake-batch-id-1"), buildEvent(ok));
    kafkaTemplate.send(inboundTopic, buildKeyRecord("fake-batch-id-2"), buildEvent(fine));

    // then
    await().pollDelay(Duration.ofSeconds(1))
        .untilAsserted(() -> assertThat(pollDlt(Duration.ofSeconds(2))).isEmpty());

    verify(dummyService, times(2)).doSomething(any(Transaction.class));
  }
}
