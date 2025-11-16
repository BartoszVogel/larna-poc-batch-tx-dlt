package pl.larna.kafka.batch.kafka_batch_demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
public class KafkaBatchDemoApplication {

  public static void main(String[] args) {
    SpringApplication.run(KafkaBatchDemoApplication.class, args);
  }

}
