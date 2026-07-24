package br.com.acme.camunda_async_events.outbox;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, Long> {

	List<OutboxMessage> findByStatusOrderById(OutboxStatus status);
}
