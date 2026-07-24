package br.com.acme.camunda_async_events.consumer;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedTransactionRepository extends JpaRepository<ProcessedTransaction, ProcessedTransactionId> {
}
