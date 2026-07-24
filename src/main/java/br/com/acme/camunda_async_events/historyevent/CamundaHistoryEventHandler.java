package br.com.acme.camunda_async_events.historyevent;

import br.com.acme.camunda_async_events.outbox.OutboxMessageRepository;
import br.com.acme.camunda_async_events.outbox.OutboxRelay;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.impl.history.event.HistoryEvent;
import org.camunda.bpm.engine.impl.history.handler.HistoryEventHandler;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;

/**
 * Ponte entre o engine do Camunda e o outbox: liga uma {@link HistoryEventTransactionSynchronization}
 * à transação Spring corrente e delega todos os eventos de história recebidos a ela.
 *
 * <p>Diferente do padrão singleton estático + {@code ApplicationContextAware} normalmente
 * necessário quando o engine é montado "na mão", aqui basta um {@code @Component} comum:
 * o {@code camunda-bpm-spring-boot-starter} monta o {@link org.camunda.bpm.engine.ProcessEngine}
 * como mais um bean Spring, então esta classe pode ser injetada normalmente via
 * {@link CamundaHistoryEventHandlerPlugin}.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class CamundaHistoryEventHandler implements HistoryEventHandler {

	private final OutboxMessageRepository outboxMessageRepository;
	private final OutboxRelay outboxRelay;
	private final ObjectMapper objectMapper;

	@Override
	public void handleEvent(HistoryEvent historyEvent) {
		if (!TransactionSynchronizationManager.isSynchronizationActive()) {
			log.warn("Evento de historico recebido fora de uma transacao ativa, ignorando: {}", historyEvent);
			return;
		}

		resolveSynchronization().addEvent(historyEvent);
	}

	@Override
	public void handleEvents(List<HistoryEvent> historyEvents) {
		historyEvents.forEach(this::handleEvent);
	}

	private HistoryEventTransactionSynchronization resolveSynchronization() {
		if (TransactionSynchronizationManager.hasResource(HistoryEventTransactionSynchronization.RESOURCE_KEY)) {
			return (HistoryEventTransactionSynchronization) TransactionSynchronizationManager.getResource(HistoryEventTransactionSynchronization.RESOURCE_KEY);
		}

		HistoryEventTransactionSynchronization synchronization = new HistoryEventTransactionSynchronization(
				outboxMessageRepository, outboxRelay, objectMapper);
		TransactionSynchronizationManager.bindResource(HistoryEventTransactionSynchronization.RESOURCE_KEY, synchronization);
		TransactionSynchronizationManager.registerSynchronization(synchronization);
		return synchronization;
	}
}
