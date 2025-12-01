package pl.larna.kafka.batch.kafka_batch_demo;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "app.kafka")
public class AppKafkaProperties {

  private BackOff backOff;
  private Inbound inbound;
  private Outbound outbound;
  private String schemaRegistryUrl;

  public Inbound getInbound() {
    return inbound;
  }

  public Outbound getOutbound() {
    return outbound;
  }

  public void setInbound(Inbound inbound) {
    this.inbound = inbound;
  }

  public void setOutbound(Outbound outbound) {
    this.outbound = outbound;
  }

  public BackOff getBackOff() {
    return backOff;
  }

  public void setBackOff(BackOff backOff) {
    this.backOff = backOff;
  }

  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  public void setSchemaRegistryUrl(String schemaRegistryUrl) {
    this.schemaRegistryUrl = schemaRegistryUrl;
  }

  public static class Inbound {

    private Topic transactionRejected;

    public Topic getTransactionRejected() {
      return transactionRejected;
    }

    public void setTransactionRejected(Topic transactionRejected) {
      this.transactionRejected = transactionRejected;
    }
  }

  public static class Outbound {

    private Topic transactionRejectedDlt;

    public Topic getTransactionRejectedDlt() {
      return transactionRejectedDlt;
    }

    public void setTransactionRejectedDlt(
        Topic transactionRejectedDlt) {
      this.transactionRejectedDlt = transactionRejectedDlt;
    }
  }

  public static class Topic {

    private String topic;

    public String getTopic() {
      return topic;
    }

    public void setTopic(String topic) {
      this.topic = topic;
    }
  }

  public static class BackOff {

    private long interval = 0;
    private long maxAttempts = 0;

    public long getInterval() {
      return interval;
    }

    public void setInterval(long interval) {
      this.interval = interval;
    }

    public long getMaxAttempts() {
      return maxAttempts;
    }

    public void setMaxAttempts(long maxAttempts) {
      this.maxAttempts = maxAttempts;
    }
  }
}
