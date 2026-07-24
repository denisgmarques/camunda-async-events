package br.com.acme.camunda_async_events.rabbitmq;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "camunda.events.rabbitmq")
public class CamundaEventsRabbitProperties {

	private String exchange = "camunda.events";
	private String queue = "camunda.events.queue";
	private String retryExchange = "camunda.events.retry";
	private String retryQueue = "camunda.events.retry.queue";
	private String dlqExchange = "camunda.events.dlq";
	private String dlqQueue = "camunda.events.dlq.queue";

	/** Tempo de espera na fila de retry antes de tentar novamente na fila principal. */
	private Duration retryDelay = Duration.ofSeconds(10);

	/** Quantidade de tentativas na fila principal antes de mandar para a DLQ definitiva. */
	private int maxRetries = 5;

	/** Tempo máximo de espera pela confirmação (publisher confirm) do broker por mensagem. */
	private Duration publishConfirmTimeout = Duration.ofSeconds(5);

	/**
	 * Idade mínima (desde {@code createdAt}) que uma linha do outbox precisa ter pra a
	 * varredura agendada ({@link br.com.acme.camunda_async_events.outbox.OutboxRelay#relayPendingMessages()})
	 * considerá-la. Não é sobre correção — o caminho de baixa latência já publica cada linha
	 * segundos depois de escrita, então a varredura tocar numa linha muito nova quase sempre é
	 * desperdício (ou, na pior hipótese, uma corrida inofensiva com o próprio caminho rápido).
	 * O padrão (3x o {@code publishConfirmTimeout}) dá margem confortável pro caminho rápido
	 * terminar sozinho antes da varredura sequer olhar pra linha, sem atrasar demais o caso que
	 * a varredura existe pra cobrir: recuperar uma linha órfã depois de um crash.
	 */
	private Duration relayMinAge = Duration.ofSeconds(15);
}
