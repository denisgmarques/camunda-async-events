package br.com.acme.camunda_async_events.historyevent;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mensagem publicada para o RabbitMQ com o resultado agregado dos eventos de história de
 * uma instância de processo. É um DTO próprio (não estende classes internas do engine
 * Camunda) para manter o contrato de integração estável entre upgrades do Camunda.
 */
@Getter
@Setter
public class ProcessInstanceEventMessage {

	/**
	 * Identifica todas as mensagens geradas a partir da mesma transação/commit do Camunda.
	 * Usado pelo consumidor para deduplicar reentregas (idempotência).
	 */
	private String transactionId;

	private String processInstanceId;
	private String processDefinitionKey;
	private String processDefinitionId;
	private String processDefinitionName;
	private Integer processDefinitionVersion;
	private String businessKey;
	private String state;
	private Date startTime;
	private Date endTime;
	private Long durationInMillis;

	private List<TaskEventMessage> tasks = new ArrayList<>();
	private Map<String, Object> variables = new HashMap<>();
}
