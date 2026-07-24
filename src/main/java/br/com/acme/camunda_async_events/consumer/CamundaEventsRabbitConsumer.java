package br.com.acme.camunda_async_events.consumer;

import br.com.acme.camunda_async_events.historyevent.ProcessInstanceEventMessage;
import br.com.acme.camunda_async_events.rabbitmq.CamundaEventsRabbitProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Consome os eventos de processo publicados pelo {@code OutboxRelay}.
 *
 * <p>Idempotência: cada mensagem carrega o {@code transactionId} gerado no momento em que o
 * evento foi capturado no Camunda (ver {@code HistoryEventTransactionSynchronization}). Uma
 * única transação pode gerar mais de uma mensagem — por exemplo processo pai e processo
 * filho de uma CallActivity capturados no mesmo commit — então a chave de dedup é o par
 * (transactionId, processInstanceId), não o transactionId sozinho. Antes de processar,
 * verificamos se esse par já está em {@code processed_transaction}; a constraint de chave
 * primária é quem garante a idempotência de fato sob reentregas concorrentes, a checagem
 * prévia é só um atalho para não repetir trabalho à toa.
 *
 * <p>Retry: falhas de processamento são rejeitadas sem requeue, o que aciona o
 * dead-lettering configurado na fila principal (volta em {@code retryDelay} pela fila de
 * retry). O número de vezes que a mensagem já passou pela fila principal é lido do
 * cabeçalho {@code x-death}; ao atingir {@code maxRetries} ela é publicada manualmente na
 * DLQ em vez de ser rejeitada de novo.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class CamundaEventsRabbitConsumer {

	private final ProcessedTransactionRepository processedTransactionRepository;
	private final ObjectMapper objectMapper;
	private final RabbitTemplate rabbitTemplate;
	private final CamundaEventsRabbitProperties properties;

	@RabbitListener(queues = "${camunda.events.rabbitmq.queue}")
	public void onMessage(Message message) {
		ProcessInstanceEventMessage event;
		try {
			event = objectMapper.readValue(message.getBody(), ProcessInstanceEventMessage.class);
		}
		catch (IOException e) {
			log.error("Mensagem ilegivel recebida, enviando direto para a DLQ", e);
			publishToDlq(message);
			return;
		}

		try {
			processIdempotently(event);
		}
		catch (Exception e) {
			handleFailure(message, event, e);
		}
	}

	private void processIdempotently(ProcessInstanceEventMessage event) {
		ProcessedTransactionId id = new ProcessedTransactionId(event.getTransactionId(), event.getProcessInstanceId());

		if (processedTransactionRepository.existsById(id)) {
			log.info("Mensagem (transacao={}, processInstance={}) ja processada, ignorando reentrega",
					event.getTransactionId(), event.getProcessInstanceId());
			return;
		}

		// Ponto de extensão: aqui entraria o processamento de negócio real do evento.
		log.info("Processando evento do processo '{}' (instancia={}, transacao={})", event.getProcessDefinitionKey(),
				event.getProcessInstanceId(), event.getTransactionId());

		try {
			processedTransactionRepository.save(new ProcessedTransaction(id));
		}
		catch (DataIntegrityViolationException concurrentDuplicate) {
			log.info("Mensagem (transacao={}, processInstance={}) foi marcada como processada por uma entrega concorrente",
					event.getTransactionId(), event.getProcessInstanceId());
		}
	}

	private void handleFailure(Message message, ProcessInstanceEventMessage event, Exception cause) {
		String transactionId = event != null ? event.getTransactionId() : "desconhecida";
		int attemptsSoFar = countMainQueueDeaths(message) + 1;

		if (attemptsSoFar >= properties.getMaxRetries()) {
			log.error("Transacao {} falhou {} vez(es), enviando para a DLQ", transactionId, attemptsSoFar, cause);
			publishToDlq(message);
			return;
		}

		log.warn("Falha processando transacao {} (tentativa {}/{}); nova tentativa em {}s", transactionId,
				attemptsSoFar, properties.getMaxRetries(), properties.getRetryDelay().toSeconds(), cause);
		throw new AmqpRejectAndDontRequeueException(cause);
	}

	private int countMainQueueDeaths(Message message) {
		List<Map<String, ?>> xDeath = message.getMessageProperties().getXDeathHeader();
		if (xDeath == null) {
			return 0;
		}

		return xDeath.stream()
				.filter(death -> properties.getQueue().equals(death.get("queue")))
				.findFirst()
				.map(death -> ((Number) death.get("count")).intValue())
				.orElse(0);
	}

	private void publishToDlq(Message message) {
		rabbitTemplate.send(properties.getDlqExchange(), properties.getDlqQueue(), message);
	}
}
