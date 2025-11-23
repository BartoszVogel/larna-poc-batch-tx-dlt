package pl.larna.kafka.batch.kafka_batch_demo;

import static java.lang.String.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClient;

//@Configuration
class KafkaSchemaRegistryConfiguration {

  private static final Logger log = LoggerFactory.getLogger(KafkaSchemaRegistryConfiguration.class);

  @Value("${app.kafka.schema.register.url}")
  private String schemaRegistryUrl;

  private final AppKafkaProperties appKafkaProperties;

  KafkaSchemaRegistryConfiguration(AppKafkaProperties appKafkaProperties) {
    this.appKafkaProperties = appKafkaProperties;
  }

  @EventListener(ApplicationReadyEvent.class)
  void registerSchemaOnStartup() {
    try {
      String schema = loadAndMinifyAvroSchema("avro/batch-transaction-event.avsc");
      updateSchema(appKafkaProperties.getInbound().getTransactionRejected().getTopic(), schema);
    } catch (Exception ex) {
      log.error("Error: not able to register avro schema: {}", ex.getMessage());
    }
  }

  private void updateSchema(String subject, String schema) {
    var webClient = WebClient.builder()
        .baseUrl(schemaRegistryUrl)
        .defaultHeader("Content-Type", "application/vnd.schemaregistry.v1+json")
        .build();

    String uri = format("/subjects/%s/versions", subject + "-value");

    ResponseEntity<String> response = webClient
        .post()
        .uri(uri)
        .bodyValue(schema)
        .retrieve()
        .toEntity(String.class)
        .block();
    assert response != null;
    log.info("Schema register response: {}", response.getBody());
    if (response.getStatusCode().is2xxSuccessful()) {
      log.info("Schema registered successfully.");
    } else {
      log.info("Failed to register schema: {}", response.getBody());
    }
  }

  private String loadAndMinifyAvroSchema(String path) throws IOException {
    // Load Avro schema file in a way that works both from IDE and packaged JAR
    var resource = new ClassPathResource(path);
    String schemaJson;
    try (var is = resource.getInputStream()) {
      schemaJson = new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }

    // Minify by removing newlines and tabs, keep single spaces to avoid accidental token merging
    String minifiedAvroSchema = schemaJson
        .replaceAll("\\r?\\n", " ")
        .replace("\t", " ")
        .trim();

    // Build payload using Jackson to ensure proper JSON escaping
    var payload = Map.of("schema", minifiedAvroSchema);
    return new ObjectMapper().writeValueAsString(payload);
  }
}
