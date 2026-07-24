package br.com.acme.camunda_async_events.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Reenvia periodicamente tudo que ainda está PENDING na tabela outbox. É o único caminho
 * que publica no RabbitMQ (nada mais chama o {@link OutboxPublisher} diretamente), o que
 * evita a corrida de envio duplicado: tanto o disparo imediato pós-commit
 * ({@link #triggerAsync()}) quanto o ciclo agendado {@link #relayPendingMessages()} caem no
 * mesmo método sincronizado, então nunca rodam ao mesmo tempo dentro desta instância.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class OutboxRelay {

	private final OutboxMessageRepository repository;
	private final OutboxPublisher publisher;

	@Scheduled(fixedDelayString = "${camunda.events.rabbitmq.relay-interval-ms:5000}")
	public synchronized void relayPendingMessages() {
		List<OutboxMessage> pending = repository.findByStatusOrderById(OutboxStatus.PENDING);
		if (pending.isEmpty()) {
			return;
		}

		log.debug("Relay encontrou {} mensagem(ns) outbox pendente(s)", pending.size());
		pending.forEach(message -> publisher.publish(message.getId()));
	}

	/** Disparo de baixa latência logo após o commit da transação do Camunda. */
	@Async
	public void triggerAsync() {
		relayPendingMessages();
	}
}
