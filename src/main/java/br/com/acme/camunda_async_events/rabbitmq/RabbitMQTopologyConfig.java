package br.com.acme.camunda_async_events.rabbitmq;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topologia de filas para os eventos do Camunda, seguindo o padrão clássico de retry com
 * atraso no RabbitMQ (sem plugins extras):
 *
 * <pre>
 * camunda.events (topic)  --#-->  camunda.events.queue
 *                                     | (falha do consumer -> nack sem requeue)
 *                                     v
 * camunda.events.retry (direct) --> camunda.events.retry.queue (TTL = retryDelay)
 *                                     | (TTL expira -> dead-letter automático)
 *                                     v
 *                          volta para camunda.events.queue
 * </pre>
 *
 * O consumidor conta quantas vezes a mensagem já passou pela fila principal (cabeçalho
 * {@code x-death}); ao atingir {@code maxRetries} ele publica manualmente na
 * {@code camunda.events.dlq.queue} em vez de deixar o ciclo se repetir.
 */
@Configuration
@EnableConfigurationProperties(CamundaEventsRabbitProperties.class)
public class RabbitMQTopologyConfig {

	@Bean
	public TopicExchange camundaEventsExchange(CamundaEventsRabbitProperties properties) {
		return new TopicExchange(properties.getExchange(), true, false);
	}

	@Bean
	public Queue camundaEventsQueue(CamundaEventsRabbitProperties properties) {
		return QueueBuilder.durable(properties.getQueue())
				.withArgument("x-dead-letter-exchange", properties.getRetryExchange())
				.withArgument("x-dead-letter-routing-key", properties.getRetryQueue())
				.build();
	}

	@Bean
	public Binding camundaEventsBinding(Queue camundaEventsQueue, TopicExchange camundaEventsExchange) {
		return BindingBuilder.bind(camundaEventsQueue).to(camundaEventsExchange).with("#");
	}

	@Bean
	public DirectExchange camundaEventsRetryExchange(CamundaEventsRabbitProperties properties) {
		return new DirectExchange(properties.getRetryExchange(), true, false);
	}

	@Bean
	public Queue camundaEventsRetryQueue(CamundaEventsRabbitProperties properties) {
		return QueueBuilder.durable(properties.getRetryQueue())
				.withArgument("x-message-ttl", properties.getRetryDelay().toMillis())
				.withArgument("x-dead-letter-exchange", properties.getExchange())
				.withArgument("x-dead-letter-routing-key", properties.getQueue())
				.build();
	}

	@Bean
	public Binding camundaEventsRetryBinding(Queue camundaEventsRetryQueue, DirectExchange camundaEventsRetryExchange,
			CamundaEventsRabbitProperties properties) {
		return BindingBuilder.bind(camundaEventsRetryQueue).to(camundaEventsRetryExchange)
				.with(properties.getRetryQueue());
	}

	@Bean
	public DirectExchange camundaEventsDlqExchange(CamundaEventsRabbitProperties properties) {
		return new DirectExchange(properties.getDlqExchange(), true, false);
	}

	@Bean
	public Queue camundaEventsDlqQueue(CamundaEventsRabbitProperties properties) {
		return QueueBuilder.durable(properties.getDlqQueue()).build();
	}

	@Bean
	public Binding camundaEventsDlqBinding(Queue camundaEventsDlqQueue, DirectExchange camundaEventsDlqExchange,
			CamundaEventsRabbitProperties properties) {
		return BindingBuilder.bind(camundaEventsDlqQueue).to(camundaEventsDlqExchange)
				.with(properties.getDlqQueue());
	}
}
