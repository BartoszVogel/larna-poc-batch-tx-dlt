package pl.larna.kafka.batch.kafka_batch_demo.rest;

import com.translation.avro.TransactionEvent;
import java.util.UUID;
import java.util.random.RandomGenerator;
import java.util.stream.IntStream;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import pl.larna.kafka.batch.kafka_batch_demo.AppKafkaProperties;
import pl.larna.kafka.batch.kafka_batch_demo.service.Transaction;
import pl.larna.kafka.batch.kafka_batch_demo.service.TransactionMapper;

@RestController
@RequestMapping("/generate-batches")
class GenerateBatchesController {

  private final KafkaTemplate<String, TransactionEvent> producer;
  private final TransactionMapper mapper;
  private final AppKafkaProperties appKafkaProperties;

  public GenerateBatchesController(
      KafkaTemplate<String, TransactionEvent> producerKafkaTemplate, TransactionMapper mapper,
      AppKafkaProperties appKafkaProperties) {
    this.producer = producerKafkaTemplate;
    this.mapper = mapper;
    this.appKafkaProperties = appKafkaProperties;
  }

  @PostMapping
  String send(@RequestBody BatchRequest message) {
    String to = appKafkaProperties.getInbound().getTransactionRejected().getTopic();
    IntStream.range(0, message.numberOfTransactions)
        .forEach(
            _ -> producer.send(to, UUID.randomUUID().toString(), generateBatch())
        );
    return "OK";
  }

  private TransactionEvent generateBatch() {
    return mapper.transactionToTransactionEvent(generateTransaction());
  }

  private Transaction generateTransaction() {
    return new Transaction(UUID.randomUUID().toString(),
        RandomGenerator.getDefault().nextDouble() * 1000,
        "Transaction");
  }

  record BatchRequest(int numberOfTransactions) {

  }
}
