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

	/**
	 * Concorrência do {@code @RabbitListener} do consumidor, no formato aceito pelo Spring AMQP
	 * ({@code "min-max"} ou um número fixo). O padrão do Spring AMQP sem essa configuração é
	 * <b>uma única thread consumidora</b> — descoberto sob carga real (ver
	 * {@code loadtest/stress-test.js}): produção sustentando ~700 msg/s contra 1 consumidor
	 * fazendo 2 idas ao banco por mensagem (checagem de idempotência + insert) empacou a fila
	 * principal em centenas de milhares de mensagens, com o RabbitMQ gastando CPU alto só
	 * gerenciando o acúmulo.
	 */
	private String consumerConcurrency = "5-10";
}
