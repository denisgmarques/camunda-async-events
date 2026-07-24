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
}
