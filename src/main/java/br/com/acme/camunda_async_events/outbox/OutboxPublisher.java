package br.com.acme.camunda_async_events.outbox;

import br.com.acme.camunda_async_events.rabbitmq.CamundaEventsRabbitProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
	private final MeterRegistry meterRegistry;

	/**
	 * Caminho da varredura agendada: recebe só o id, relê do banco antes de publicar. A
	 * releitura importa aqui porque esse é o caminho que pode disputar a mesma linha com outra
	 * JVM (ou com o disparo de baixa latência, dentro desta mesma JVM) — o SELECT é o que
	 * permite detectar que a linha já sumiu entre a consulta ampla do
	 * {@link OutboxRelay#relayPendingMessages()} e esta chamada, e pular sem tentar de novo.
	 *
	 * <p>Apaga via {@link OutboxMessageRepository#deleteById(Long)} (o {@code DELETE} em JPQL
	 * sobrescrito), <b>não</b> via {@code repository.delete(message)}: o delete por entidade do
	 * Spring Data confere a contagem de linhas afetadas e lança {@code OptimisticLockException}
	 * se a linha já não existir mais — o que aconteceria exatamente na corrida que este método
	 * está preparado pra tolerar (outra chamada publicou e apagou a linha entre o SELECT acima e
	 * este DELETE). O {@code deleteById} sobrescrito não faz essa checagem, então uma segunda
	 * tentativa de apagar uma linha já apagada é um no-op silencioso, como deveria ser.
	 */
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void publish(Long outboxMessageId) {
		OutboxMessage message = repository.findById(outboxMessageId).orElse(null);
		if (message == null) {
			// Já foi enviada e apagada (ex.: corrida entre o disparo imediato e o ciclo agendado).
			return;
		}

		if (publishWithConfirm(message)) {
			repository.deleteById(outboxMessageId);
		}
		else {
			log.warn("RabbitMQ nao confirmou a mensagem outbox {} a tempo; sera retentada", outboxMessageId);
		}
	}

	/**
	 * Caminho de baixa latência pós-commit: recebe a entidade que a própria transação acabou
	 * de gravar (ainda com o payload em memória, sem SELECT). Não confere se a linha ainda
	 * existe antes de publicar — de propósito, ver o javadoc de {@link OutboxRelay}. Na rara
	 * corrida onde a varredura agendada já publicou e apagou esta mesma linha antes desta
	 * chamada rodar, o pior cenário é uma publicação duplicada no RabbitMQ (absorvida pelo
	 * consumidor idempotente) seguida de um {@code DELETE} que não bate em linha nenhuma — o
	 * {@link OutboxMessageRepository#deleteById(Long)} sobrescrito é um {@code DELETE} puro em
	 * JPQL, não lança exceção quando zero linhas são afetadas.
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

	/**
	 * {@code outbox_publish_confirm_seconds}: quanto tempo cada publicação gastou esperando o
	 * publisher-confirm do RabbitMQ, com a tag {@code confirmed} separando o que foi confirmado
	 * do que não foi (timeout ou erro). Esse é o ponto mais provável de virar gargalo sob carga —
	 * é a única espera de rede síncrona no caminho inteiro do outbox.
	 */
	private boolean publishWithConfirm(OutboxMessage message) {
		Timer.Sample sample = Timer.start(meterRegistry);
		boolean confirmed = false;
		try {
			Boolean result = rabbitTemplate.invoke(operations -> {
				operations.convertAndSend(properties.getExchange(), message.getProcessDefinitionKey(),
						message.getPayload(), asJsonMessage());
				return operations.waitForConfirms(properties.getPublishConfirmTimeout().toMillis());
			});
			confirmed = Boolean.TRUE.equals(result);
			return confirmed;
		}
		catch (Exception e) {
			log.error("Falha publicando mensagem outbox {} - sera retentada no proximo ciclo", message.getId(), e);
			return false;
		}
		finally {
			sample.stop(meterRegistry.timer("outbox.publish.confirm", "confirmed", String.valueOf(confirmed)));
		}
	}

	private MessagePostProcessor asJsonMessage() {
		return message -> {
			message.getMessageProperties().setContentType(MessageProperties.CONTENT_TYPE_JSON);
			return message;
		};
	}
}
