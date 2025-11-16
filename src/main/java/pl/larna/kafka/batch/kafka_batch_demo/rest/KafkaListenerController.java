package pl.larna.kafka.batch.kafka_batch_demo.rest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/debug/kafka-listeners")
class KafkaListenerController {

  private final KafkaListenerEndpointRegistry registry;

  KafkaListenerController(KafkaListenerEndpointRegistry registry) {
    this.registry = registry;
  }

  @PostMapping("/{id}/start")
  ResponseEntity<String> start(@PathVariable String id) {
    MessageListenerContainer container = registry.getListenerContainer(id);
    if (container == null) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND)
          .body("Listener with id '%s' not found".formatted(id));
    }
    if (!container.isRunning()) {
      container.start();
    }
    return ResponseEntity.ok("Listener '%s' is running".formatted(id));
  }

  @PostMapping("/{id}/stop")
  ResponseEntity<String> stop(@PathVariable String id) {
    MessageListenerContainer container = registry.getListenerContainer(id);
    if (container == null) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND)
          .body("Listener with id '%s' not found".formatted(id));
    }
    if (container.isRunning()) {
      container.stop();
    }
    return ResponseEntity.ok("Listener '%s' is stopped".formatted(id));
  }

  @GetMapping("/{id}")
  ResponseEntity<String> status(@PathVariable String id) {
    MessageListenerContainer container = registry.getListenerContainer(id);
    if (container == null) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND)
          .body("Listener with id '%s' not found".formatted(id));
    }
    return ResponseEntity.ok(container.isRunning() ? "running" : "stopped");
  }
}
