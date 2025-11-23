package pl.larna.kafka.batch.kafka_batch_demo;

import java.nio.charset.StandardCharsets;
import java.util.function.BiFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationUtils;

/**
 * A ConsumerRecordRecoverer that publishes failed records to a DLT as raw bytes. It converts the
 * key/value to byte[]; if a DeserializationException header is present, it prefers the original
 * record bytes from that header.
 */
class ByteArrayDeadLetterRecoverer implements ConsumerRecordRecoverer {

  private static final LogAccessor logger = new LogAccessor(ByteArrayDeadLetterRecoverer.class);
  private static final Logger log = LoggerFactory.getLogger(ByteArrayDeadLetterRecoverer.class);

  private final KafkaTemplate<byte[], byte[]> template;
  private final BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver;

  ByteArrayDeadLetterRecoverer(KafkaTemplate<byte[], byte[]> template,
      BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver) {
    this.template = template;
    this.destinationResolver = destinationResolver;
  }

  @Override
  public void accept(ConsumerRecord<?, ?> record, Exception exception) {
    TopicPartition tp = destinationResolver.apply(record, exception);
    Headers headers = copyHeaders(record.headers());

    // Convert to bytes (raw if already byte[]; otherwise UTF-8/toString())
    byte[] keyBytes = extractKeyBytes(record);
    log.warn("Extract key from record: {}", new String(keyBytes, StandardCharsets.UTF_8));
    byte[] valueBytes = extractValueBytes(record);

    // If the source record has a negative timestamp (e.g., synthetic ConsumerRecord),
    // pass null so the broker assigns the timestamp. Kafka forbids negative timestamps.
    Long ts = record.timestamp() >= 0 ? record.timestamp() : null;
    ProducerRecord<byte[], byte[]> pr = new ProducerRecord<>(tp.topic(), tp.partition(),
        ts, keyBytes, valueBytes, headers);

    template.send(pr).whenComplete((result, ex) -> {
      if (ex == null) {
        long offset = result != null && result.getRecordMetadata() != null
            ? result.getRecordMetadata().offset() : -1L;
        log.debug("Sent to DLT topic={} partition={} offset={}", tp.topic(), tp.partition(),
            offset);
      } else {
        log.error("Failed to publish to DLT topic={} partition={} cause={}", tp.topic(),
            tp.partition(), ex.getMessage());
      }
    });
  }

  private Headers copyHeaders(Headers original) {
    RecordHeaders copy = new RecordHeaders();
    original.forEach(h -> copy.add(h.key(), h.value()));
    // mark DLT and add own record headers
    copy.add("x-original-topic", nullSafeBytes(original.lastHeader(KafkaHeaders.RECEIVED_TOPIC)));
    copy.add("x-error-class", "dlt".getBytes(StandardCharsets.UTF_8));
    return copy;
  }

  private byte[] nullSafeBytes(Header header) {
    return getBytes(header);
  }

  private byte[] extractKeyBytes(ConsumerRecord<?, ?> record) {
    return getBytes(record.key());
  }

  private byte[] extractValueBytes(ConsumerRecord<?, ?> record) {
    Header header = record.headers().lastHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER);
    if (header != null) {
      byte[] value = {};
      try {
        DeserializationException exception = SerializationUtils.byteArrayToDeserializationException(
            logger, header);
        if (exception != null) {
          log.debug("DeserializationException data: {}", new String(value, StandardCharsets.UTF_8));
          return exception.getData();
        }
      } catch (Exception ex) {
        log.error("DeserializationException could not be deserialized", ex);
      }
      return value;
    } else {
      return getBytes(record.value());
    }
  }

  private static byte[] getBytes(Object value) {
    return switch (value) {
      case null -> null;
      case byte[] b -> b;
      case String s -> s.getBytes(StandardCharsets.UTF_8);
      default -> value.toString().getBytes(StandardCharsets.UTF_8);
    };
  }
}
