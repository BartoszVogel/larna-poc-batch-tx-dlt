package pl.larna.kafka.batch.kafka_batch_demo.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.translation.avro.BatchTransactionEvent;
import com.translation.avro.Transaction;
import java.util.List;
import java.util.UUID;
import java.util.random.RandomGenerator;
import java.util.stream.IntStream;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import pl.larna.kafka.batch.kafka_batch_demo.AppKafkaProperties;

@RestController
@RequestMapping("/generate-batches")
class GenerateBatchesController {

  private final KafkaTemplate<String, BatchTransactionEvent> producer;
  private final AppKafkaProperties appKafkaProperties;

  public GenerateBatchesController(
      KafkaTemplate<String, BatchTransactionEvent> producerKafkaTemplate,
      AppKafkaProperties appKafkaProperties) {
    this.producer = producerKafkaTemplate;
    this.appKafkaProperties = appKafkaProperties;
  }

  @PostMapping
  String send(@RequestBody BatchRequest message) {
    IntStream.range(0, message.numberOfBatches)
        .forEach(_ -> {
          var event = generateBatch(message.transactionInBatch, message.shouldHasError);
          producer.send(appKafkaProperties.getInbound().getTransactionRejected().getTopic(),
              UUID.randomUUID().toString(), event);
        });
    return "OK";
  }

  private BatchTransactionEvent generateBatch(int transactionInBatch, boolean shouldHasError) {
    List<Transaction> transactions = generateTransactions(transactionInBatch, shouldHasError);
    return BatchTransactionEvent.newBuilder()
        .setBatchId(UUID.randomUUID().toString())
        .setTransactions(transactions)
        .build();
  }

  private List<Transaction> generateTransactions(int transactionCount, boolean shouldHasError) {
    int errorIndex = shouldHasError ? transactionCount / 2 : -1;
    return IntStream.range(0, transactionCount)
        .mapToObj(index -> {
          String description = errorIndex == index ? "Error" : "Transaction_" + index;
          return generateTransaction(description);
        })
        .toList();
  }

  private static Transaction generateTransaction(String description) {
    return Transaction.newBuilder()
        .setTransactionId(UUID.randomUUID().toString())
        .setAmount(RandomGenerator.getDefault().nextDouble() * 1000)
        .setDescription(description)
        .build();
  }

  record BatchRequest(int numberOfBatches, int transactionInBatch,
                      @JsonProperty("containError") boolean shouldHasError) {

  }
}
