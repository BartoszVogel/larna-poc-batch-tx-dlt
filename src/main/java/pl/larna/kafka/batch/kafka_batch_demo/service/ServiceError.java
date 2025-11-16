package pl.larna.kafka.batch.kafka_batch_demo.service;

public class ServiceError extends RuntimeException {

  public ServiceError(String message) {
    super(message);
  }
}
