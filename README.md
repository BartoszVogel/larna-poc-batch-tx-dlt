# Kafka Batch Demo with Avro, Batch Listener and Dead Letter Topic (DLT)

## Overview
This Spring Boot app demonstrates:
- Consuming Kafka messages in batches with manual acknowledgments.
- Producing Avro records (value) with a String key.
- Treating deserialization/processing errors as non‑retriable and routing them directly to a Dead Letter Topic (DLT) as raw bytes.
- Simple REST endpoints to generate sample batches and control the Kafka listener.

## Prerequisites
- Java 25
- Maven 3.9+
- Docker (to run Kafka + Schema Registry locally)

## Start Kafka and Schema Registry
1) From the project root, start the services:

```bash
docker compose up -d
```

2) Default endpoints after startup:
- Kafka broker: `localhost:9092`
- Schema Registry: `http://localhost:8081`

## Build and run the application
1) Build:

```bash
mvn clean package
```

2) Run:

```bash
mvn spring-boot:run
```

Or run `KafkaBatchDemoApplication` from your IDE.

## Key configuration (application.yml)
- `spring.kafka.bootstrap-servers`: `localhost:9092`
- Consumer settings:
  - Batch mode with manual ack (`ack-mode: manual`)
  - Avro value deserializer (`KafkaAvroDeserializer`) with `specific.avro.reader=true`
  - Group id: `rejected-transactions-service`
- Topics:
  - `app.kafka.consumer.transaction-rejected.topic`: `local.rejected-transactions-service.transaction-rejected`
  - `app.kafka.consumer.transaction-rejected-dlt.topic`: `local.rejected-transactions-service.transaction-rejected.dlt`
- Schema Registry URL:
  - `app.kafka.schema.register.url`: `http://localhost:8081`

## About Avro and Schema Registry
- Producer (used by the controller) serializes values with `KafkaAvroSerializer`.
- If you don’t want producers to create new schemas automatically, set `auto.register.schemas=false` and ensure the schema exists in the registry for the subject `<topic>-value`.
- A helper (`KafkaSchemaRegistryConfiguration`) exists to register the Avro schema on startup; adjust it in your environment if needed.

## REST endpoints

### 1) Generate sample batches
- POST `/generate-batches`
- Request body example:

```json
{
  "numberOfBatches": 2,
  "transactionInBatch": 5,
  "containError": true
}
```

What it does:
- Sends `numberOfBatches` Avro messages to the main topic with `transactionInBatch` items each.
- If `containError` is true, one item per batch is marked to simulate an error during processing.

### 2) Control the Kafka listener (start/stop/status)
- Base path: `/debug/kafka-listeners`
- Listener id: `transaction-rejected-listener`

Start the listener:

```bash
curl -X POST http://localhost:8080/debug/kafka-listeners/transaction-rejected-listener/start
```

Stop the listener:

```bash
curl -X POST http://localhost:8080/debug/kafka-listeners/transaction-rejected-listener/stop
```

Check status:

```bash
curl http://localhost:8080/debug/kafka-listeners/transaction-rejected-listener
```

## Error handling and DLT
- The application uses `DefaultErrorHandler` with a custom `ConsumerRecordRecoverer` that publishes failed records to the DLT as raw bytes (byte[] key/value).
- Deserialization‑related exceptions are configured as not retryable, so records that do not comply with the Avro schema are immediately routed to the DLT.
- The DLT name is `<originalTopic>.dlt` and the original partition is preserved.
- The DLT producer uses `ByteArraySerializer` for both key and value. Consumers of the DLT should not use Avro deserializers.

## How to verify end‑to‑end
1) Produce batches

Example using curl:

```bash
curl -H "Content-Type: application/json" \
  -d '{"numberOfBatches":1,"transactionInBatch":3,"containError":true}' \
  http://localhost:8080/generate-batches
```

### Produce messages from the command line

You can push messages to Kafka without the REST API. Below are ready‑to‑use snippets.

Defaults used by the app:
- Main topic: `local.rejected-transactions-service.transaction-rejected`
- DLT topic: `local.rejected-transactions-service.transaction-rejected.dlt`
- Broker: `localhost:9092`
- Schema Registry: `http://localhost:8081`

1) Produce a VALID Avro message to the main topic (requires Confluent CLI tools)

Use kafka-avro-console-producer so the value is serialized with the Schema Registry.

```bash
export BOOTSTRAP=localhost:9092
export SR=http://localhost:8081
export TOPIC=local.rejected-transactions-service.transaction-rejected

kafka-avro-console-producer \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --property schema.registry.url=$SR \
  --property value.schema='{
    "type":"record",
    "name":"BatchTransactionEvent",
    "namespace":"com.translation.avro",
    "fields":[
      {"name":"batchId","type":"string"},
      {"name":"transactions","type":{"type":"array","items":{
        "type":"record","name":"Transaction","fields":[
          {"name":"transactionId","type":"string"},
          {"name":"amount","type":"double"},
          {"name":"description","type":"string"}
        ]
      }}}
    ]
  }'
```

Then paste one JSON value per line (Ctrl+C to exit):

```json
{"batchId":"b-1","transactions":[{"transactionId":"t-1","amount":12.5,"description":"ok"},{"transactionId":"t-2","amount":7.99,"description":"ok"}]}
```

2) Produce an INVALID message to the main topic (to trigger DLT)

Send raw bytes or a JSON that does not conform to the Avro schema using the plain console producer. The consumer will fail deserialization and the record will be sent to the DLT with the original bytes as value.

```bash
export BOOTSTRAP=localhost:9092
export TOPIC=local.rejected-transactions-service.transaction-rejected

kafka-console-producer \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC"
```

Now type any invalid payload, e.g. random text or wrong JSON:

```
this-is-not-avro
{"foo":"bar"}
```

3) (Optional) Produce a RAW message directly to the DLT topic

DLT consumers expect raw byte[] values. You can write any text or bytes there for testing the DLT listener.

```bash
export BOOTSTRAP=localhost:9092
export DLT=local.rejected-transactions-service.transaction-rejected.dlt

kafka-console-producer \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$DLT"
```

Type a line and press Enter to send. The DLT listener should log receipt.

2) Observe processing logs
- The application logs the beginning and end of batch processing and any DLT publishes.

3) Consume the DLT topic (raw bytes)

If you have Kafka CLI tools installed:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic local.rejected-transactions-service.transaction-rejected.dlt \
  --from-beginning \
  --property print.key=true \
  --key-deserializer org.apache.kafka.common.serialization.ByteArrayDeserializer \
  --value-deserializer org.apache.kafka.common.serialization.ByteArrayDeserializer
```

If you know payloads are UTF‑8 text, you can use `StringDeserializer` for the value:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic local.rejected-transactions-service.transaction-rejected.dlt \
  --from-beginning \
  --property print.key=true \
  --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --value-deserializer org.apache.kafka.common.serialization.StringDeserializer
```

## Troubleshooting
- App fails to start due to missing KafkaTemplate:
  - Ensure `KafkaConfiguration` defines `KafkaTemplate<String, com.translation.avro.BatchTransactionEvent>` (it is provided by default).
- No messages in DLT:
  - Ensure the DLT topic exists (it is created via `NewTopic` bean).
  - Check logs for failed publish attempts or schema errors.
  - Confirm your DLT consumer is not using Avro deserializers.
- Producer schema errors:
  - With `auto.register.schemas=false`, ensure `<topic>-value` subject exists in Schema Registry and is compatible.

## Defaults (quick reference)
- Main topic: `local.rejected-transactions-service.transaction-rejected`
- DLT topic: `local.rejected-transactions-service.transaction-rejected.dlt`
- Schema Registry: `http://localhost:8081`
- Broker: `localhost:9092`

## License
For demo and learning purposes.
