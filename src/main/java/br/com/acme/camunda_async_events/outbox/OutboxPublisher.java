package br.com.acme.camunda_async_events.outbox;

import br.com.acme.camunda_async_events.rabbitmq.CamundaEventsRabbitProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Publica mensagens outbox no RabbitMQ, cada uma na sua própria transação
 * ({@code REQUIRES_NEW}) para que a falha ao publicar uma não desfaça o progresso já feito com
 * as demais. Só apaga a linha depois de receber a confirmação (publisher confirm) do broker —
 * sem isso ela continua na tabela e será tentada de novo no próximo ciclo do {@link OutboxRelay}.
 */
@Component
@Slf4j
@RequiredArgsConstructor
class OutboxPublisher {

	private final OutboxMessageRepository repository;
	private final RabbitTemplate rabbitTemplate;
	private final CamundaEventsRabbitProperties properties;

	/**
	 * Caminho da varredura agendada: recebe só o id, relê do banco antes de publicar. A
	 * releitura importa aqui porque esse é o caminho que pode disputar a mesma linha com outra
	 * JVM — o SELECT é o que permite detectar que a linha já sumiu (outra instância já publicou
	 * e apagou) entre a consulta ampla do {@link OutboxRelay#relayPendingMessages()} e esta
	 * chamada, e pular sem tentar de novo.
	 */
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void publish(Long outboxMessageId) {
		OutboxMessage message = repository.findById(outboxMessageId).orElse(null);
		if (message == null) {
			// Já foi enviada e apagada (ex.: corrida entre o disparo imediato e o ciclo agendado).
			return;
		}

		if (publishWithConfirm(message)) {
			repository.delete(message);
		}
		else {
			log.warn("RabbitMQ nao confirmou a mensagem outbox {} a tempo; sera retentada", outboxMessageId);
		}
	}

	/**
	 * Caminho de baixa latência pós-commit: recebe a entidade que a própria transação acabou
	 * de gravar (ainda com o payload em memória, sem SELECT). Seguro sem reconferir se a linha
	 * ainda existe porque {@link OutboxRelay#triggerAsync(List)} e
	 * {@link OutboxRelay#relayPendingMessages()} são {@code synchronized} no mesmo monitor —
	 * dentro desta JVM não existe outra chamada concorrente que possa ter publicado (e apagado)
	 * esta linha antes.
	 */
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void publish(OutboxMessage message) {
		if (publishWithConfirm(message)) {
			/**
			 * APOS ENVIAR COM SUCESSO AO RABBITMQ
			 * APAGA DO DB (OUTBOX)
			 */
			repository.deleteById(message.getId());
		}
		else {
			log.warn("RabbitMQ nao confirmou a mensagem outbox {} a tempo; sera retentada", message.getId());
		}
	}

	private boolean publishWithConfirm(OutboxMessage message) {
		try {
			Boolean confirmed = rabbitTemplate.invoke(operations -> {
				operations.convertAndSend(properties.getExchange(), message.getProcessDefinitionKey(),
						message.getPayload(), asJsonMessage());
				return operations.waitForConfirms(properties.getPublishConfirmTimeout().toMillis());
			});
			return Boolean.TRUE.equals(confirmed);
		}
		catch (Exception e) {
			log.error("Falha publicando mensagem outbox {} - sera retentada no proximo ciclo", message.getId(), e);
			return false;
		}
	}

	private MessagePostProcessor asJsonMessage() {
		return message -> {
			message.getMessageProperties().setContentType(MessageProperties.CONTENT_TYPE_JSON);
			return message;
		};
	}
}
