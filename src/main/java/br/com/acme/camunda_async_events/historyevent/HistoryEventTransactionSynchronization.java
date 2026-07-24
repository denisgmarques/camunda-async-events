package br.com.acme.camunda_async_events.historyevent;

import br.com.acme.camunda_async_events.outbox.OutboxMessage;
import br.com.acme.camunda_async_events.outbox.OutboxMessageRepository;
import br.com.acme.camunda_async_events.outbox.OutboxRelay;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.camunda.bpm.engine.impl.history.event.HistoryEvent;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Acumula os {@link HistoryEvent} emitidos pelo Camunda durante UMA transação e, no
 * {@code beforeCommit}, grava as mensagens agregadas na tabela outbox — usando o mesmo
 * {@link org.springframework.transaction.PlatformTransactionManager} (e portanto a mesma
 * conexão/transação) do comando do Camunda que está sendo commitado. Isso é o que garante
 * que o evento e a mudança de estado do processo sejam atômicos: ou os dois são
 * persistidos, ou nenhum é.
 */
class HistoryEventTransactionSynchronization implements TransactionSynchronization {

	static final String RESOURCE_KEY = HistoryEventTransactionSynchronization.class.getName();

	private final String transactionId = UUID.randomUUID().toString();
	private final List<HistoryEvent> events = new ArrayList<>();
	private final List<OutboxMessage> savedOutboxMessages = new ArrayList<>();

	private final OutboxMessageRepository outboxMessageRepository;
	private final OutboxRelay outboxRelay;
	private final ObjectMapper objectMapper;

	HistoryEventTransactionSynchronization(OutboxMessageRepository outboxMessageRepository, OutboxRelay outboxRelay,
			ObjectMapper objectMapper) {
		this.outboxMessageRepository = outboxMessageRepository;
		this.outboxRelay = outboxRelay;
		this.objectMapper = objectMapper;
	}

	void addEvent(HistoryEvent event) {
		events.add(event);
	}

	@Override
	public void beforeCommit(boolean readOnly) {
		if (events.isEmpty()) {
			return;
		}

		HistoryEventAggregator.aggregate(events, transactionId).forEach(this::saveToOutbox);
		// Já extraímos tudo que interessa (agregado e persistido); solta as referências aos
		// HistoryEvent brutos em vez de esperar o objeto inteiro morrer no afterCompletion.
		events.clear();
	}

	@Override
	public void afterCompletion(int status) {
		TransactionSynchronizationManager.unbindResourceIfPossible(RESOURCE_KEY);
		if (status == TransactionSynchronization.STATUS_COMMITTED) {
			outboxRelay.triggerAsync(savedOutboxMessages);
		}
	}

	private void saveToOutbox(ProcessInstanceEventMessage message) {
		try {
			String payload = objectMapper.writeValueAsString(message);
			OutboxMessage saved = outboxMessageRepository.save(new OutboxMessage(message.getTransactionId(),
					message.getProcessInstanceId(), message.getProcessDefinitionKey(), payload));
			savedOutboxMessages.add(saved);
		}
		catch (JsonProcessingException e) {
			throw new IllegalStateException("Falha serializando evento de historico do Camunda", e);
		}
	}
}
