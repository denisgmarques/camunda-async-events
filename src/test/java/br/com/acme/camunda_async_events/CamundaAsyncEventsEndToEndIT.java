package br.com.acme.camunda_async_events;

import br.com.acme.camunda_async_events.consumer.ProcessedTransactionId;
import br.com.acme.camunda_async_events.consumer.ProcessedTransactionRepository;
import br.com.acme.camunda_async_events.outbox.OutboxMessageRepository;
import br.com.acme.camunda_async_events.rabbitmq.CamundaEventsRabbitProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.TaskService;
import org.camunda.bpm.engine.history.HistoricProcessInstance;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.task.Task;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Prova, com uma aplicação Spring Boot e um RabbitMQ <b>real</b> (não mocks), a mesma
 * cadeia descrita no README: Camunda captura o evento na transação &rarr; grava no outbox
 * &rarr; o relay publica com publisher-confirm &rarr; o consumidor processa de forma
 * idempotente &rarr; uma reentrega da mesma mensagem é ignorada.
 *
 * <p>Sobe um RabbitMQ descartável via Testcontainers (não depende do {@code docker-compose.yml}
 * manual — só precisa de Docker disponível). Roda com {@code mvn verify} (sufixo {@code IT}),
 * não com {@code mvn test}: é mais lento que a suíte de BPMN (sobe o Spring context inteiro +
 * um container) e faz uma chamada HTTP real ao ViaCEP, de propósito — o objetivo aqui é validar
 * o pipeline de mensageria de ponta a ponta, não simular o ViaCEP.
 *
 * <p>Cada fase loga uma linha "===" para quem rodar isso conseguir acompanhar no console
 * exatamente o que está acontecendo, na ordem em que acontece.
 */
@Slf4j
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class CamundaAsyncEventsEndToEndIT {

	@Container
	static final RabbitMQContainer RABBITMQ = new RabbitMQContainer("rabbitmq:3.13-management");

	@DynamicPropertySource
	static void rabbitProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.rabbitmq.host", RABBITMQ::getHost);
		registry.add("spring.rabbitmq.port", RABBITMQ::getAmqpPort);
		registry.add("spring.rabbitmq.username", RABBITMQ::getAdminUsername);
		registry.add("spring.rabbitmq.password", RABBITMQ::getAdminPassword);
	}

	@Autowired
	private RuntimeService runtimeService;

	@Autowired
	private TaskService taskService;

	@Autowired
	private HistoryService historyService;

	@Autowired
	private OutboxMessageRepository outboxMessageRepository;

	@Autowired
	private ProcessedTransactionRepository processedTransactionRepository;

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private RabbitAdmin rabbitAdmin;

	@Autowired
	private CamundaEventsRabbitProperties rabbitmqProperties;

	@Autowired
	private ObjectMapper objectMapper;

	@Test
	void capturesRelaysAndConsumesEventsForBothParentAndChildProcessInstances() {
		log.info("=== 0) abrindo uma fila auxiliar ligada no mesmo exchange (\"#\"), so pra capturar uma "
				+ "copia crua de cada mensagem publicada - nao depende da linha do outbox sobreviver ao "
				+ "envio, ja que ela e apagada assim que o RabbitMQ confirma ===");
		// autoDelete=false de propósito: com autoDelete=true a fila some assim que o primeiro
		// receive() cancela seu consumidor interno, e o passo 4 faz varias chamadas em sequencia.
		// Sem problema deixar sobrar - e um RabbitMQ descartavel do Testcontainers, o container
		// inteiro morre no fim do teste.
		Queue captureQueue = new Queue("camunda.events.test-capture.queue", false, false, false);
		rabbitAdmin.declareQueue(captureQueue);
		rabbitAdmin.declareBinding(captureBinding(captureQueue));

		log.info("=== 1) iniciando cadastroClienteProcess (nome, cpf, cep) ===");
		ProcessInstance parent = runtimeService.startProcessInstanceByKey("cadastroClienteProcess", Map.of(
				"nome", "Teste End-to-End",
				"cpf", "999.888.777-66",
				"cep", "01001-000"));
		log.info("processInstanceId (pai) = {}", parent.getId());

		log.info("=== 2) esperando o job assincrono da CallActivity + o ViaCEP real completarem "
				+ "e o processo chegar em 'Avaliar Cadastro' ===");
		await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
			Task task = taskService.createTaskQuery()
					.processInstanceId(parent.getId())
					.taskDefinitionKey("Task_AvaliarCadastro")
					.singleResult();
			assertThat(task).as("tarefa 'Avaliar Cadastro' do processo pai").isNotNull();
		});

		HistoricProcessInstance child = historyService.createHistoricProcessInstanceQuery()
				.superProcessInstanceId(parent.getId())
				.singleResult();
		assertThat(child).as("instancia filha criada pela CallActivity (consultaCepProcess)").isNotNull();
		log.info("processInstanceId (filho, consultaCepProcess) = {}", child.getId());

		log.info("=== 3) esperando outbox -> RabbitMQ (publisher-confirm) -> consumer confirmarem "
				+ "AS DUAS processInstanceId (mesma transacao, dois processos) ===");
		await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
			List<String> processedInstances = processedTransactionRepository.findAll().stream()
					.map(pt -> pt.getId().getProcessInstanceId())
					.toList();
			assertThat(processedInstances)
					.as("processed_transaction deve conter o pai e o filho")
					.contains(parent.getId(), child.getId());
		});
		log.info("confirmado: processed_transaction tem registro para o pai E para o filho");

		await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
				assertThat(outboxMessageRepository.findAll())
						.as("outbox deveria estar vazio depois do publisher-confirm - cada linha e apagada "
								+ "assim que confirmada, entao 'nao existir mais' e o sinal de sucesso")
						.isEmpty());
		log.info("confirmado: outbox esvaziou (tudo foi confirmado pelo broker e apagado)");

		log.info("=== 4) pegando a copia crua da mensagem do filho na fila auxiliar e reenviando "
				+ "manualmente para provar a idempotencia ===");
		Message childRawMessage = findCapturedMessageForProcessInstance(captureQueue.getName(), child.getId());
		JsonNode childPayload = readPayload(childRawMessage);
		long processedCountBefore = processedTransactionRepository.count();

		rabbitTemplate.send(rabbitmqProperties.getExchange(),
				childRawMessage.getMessageProperties().getReceivedRoutingKey(), childRawMessage);

		boolean reprocessedAsDuplicate = processedTransactionRepository.existsById(new ProcessedTransactionId(
				childPayload.get("transactionId").asText(), childPayload.get("processInstanceId").asText()));
		assertThat(reprocessedAsDuplicate).isTrue();

		await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(10)).untilAsserted(() ->
				assertThat(processedTransactionRepository.count())
						.as("reentrega da mesma mensagem nao deveria criar um novo registro")
						.isEqualTo(processedCountBefore));
		log.info("confirmado: a reentrega foi ignorada, processed_transaction nao cresceu "
				+ "(continua em {} registro(s))", processedCountBefore);

		log.info("=== fim: outbox -> RabbitMQ -> consumer -> idempotencia validados de ponta a ponta ===");
	}

	private Binding captureBinding(Queue captureQueue) {
		return BindingBuilder.bind(captureQueue).to(new TopicExchange(rabbitmqProperties.getExchange())).with("#");
	}

	/** Drena a fila de captura até achar a mensagem cujo {@code processInstanceId} bate com o esperado. */
	private Message findCapturedMessageForProcessInstance(String queueName, String processInstanceId) {
		for (int attempt = 0; attempt < 5; attempt++) {
			Message message = rabbitTemplate.receive(queueName, 3000);
			if (message == null) {
				break;
			}
			if (processInstanceId.equals(readPayload(message).path("processInstanceId").asText(null))) {
				return message;
			}
		}
		throw new IllegalStateException(
				"Mensagem capturada para processInstanceId=" + processInstanceId + " nao encontrada");
	}

	private JsonNode readPayload(Message message) {
		try {
			return objectMapper.readTree(message.getBody());
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
