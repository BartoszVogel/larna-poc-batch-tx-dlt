package pl.larna.kafka.batch.kafka_batch_demo.service;

public record Transaction(String transactionId, Double amount, String description) {

}
