package pl.larna.kafka.batch.kafka_batch_demo.service;

import com.translation.avro.TransactionEvent;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface TransactionMapper {

  Transaction transactionEventToTransaction(TransactionEvent event);
  TransactionEvent transactionToTransactionEvent(Transaction transaction);

}
