package br.com.acme.camunda_async_events.historyevent;

import org.camunda.bpm.engine.impl.history.event.HistoricProcessInstanceEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricTaskInstanceEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoricVariableUpdateEventEntity;
import org.camunda.bpm.engine.impl.history.event.HistoryEvent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Agrupa os {@link HistoryEvent} emitidos pelo engine numa única mensagem por instância de
 * processo, pronta para publicação. Eventos de gateway/sequence flow são ignorados de
 * propósito: só interessam o ciclo de vida da instância, tarefas e variáveis.
 */
public final class HistoryEventAggregator {

	private HistoryEventAggregator() {
	}

	public static List<ProcessInstanceEventMessage> aggregate(List<HistoryEvent> events, String transactionId) {
		Map<String, ProcessInstanceEventMessage> messagesByProcessInstance = new LinkedHashMap<>();

		for (HistoryEvent event : events) {
			ProcessInstanceEventMessage message = messagesByProcessInstance
					.computeIfAbsent(event.getProcessInstanceId(), id -> newMessage(id, transactionId));

			applyProcessDefinitionInfo(message, event);

			if (event instanceof HistoricProcessInstanceEventEntity processInstanceEvent) {
				applyProcessInstanceFields(message, processInstanceEvent);
			}
			else if (event instanceof HistoricTaskInstanceEventEntity taskEvent) {
				message.getTasks().add(toTaskEventMessage(taskEvent));
			}
			else if (event instanceof HistoricVariableUpdateEventEntity variableEvent) {
				message.getVariables().put(variableEvent.getVariableName(), extractValue(variableEvent));
			}
		}

		return new ArrayList<>(messagesByProcessInstance.values());
	}

	private static ProcessInstanceEventMessage newMessage(String processInstanceId, String transactionId) {
		ProcessInstanceEventMessage message = new ProcessInstanceEventMessage();
		message.setProcessInstanceId(processInstanceId);
		message.setTransactionId(transactionId);
		return message;
	}

	private static void applyProcessDefinitionInfo(ProcessInstanceEventMessage message, HistoryEvent event) {
		// Nem todo HistoryEvent traz essa informação preenchida; mantém o primeiro valor encontrado.
		if (message.getProcessDefinitionKey() == null) {
			message.setProcessDefinitionKey(event.getProcessDefinitionKey());
			message.setProcessDefinitionId(event.getProcessDefinitionId());
			message.setProcessDefinitionName(event.getProcessDefinitionName());
			message.setProcessDefinitionVersion(event.getProcessDefinitionVersion());
		}
	}

	private static void applyProcessInstanceFields(ProcessInstanceEventMessage message,
			HistoricProcessInstanceEventEntity event) {
		message.setBusinessKey(event.getBusinessKey());
		message.setState(event.getState());
		message.setStartTime(event.getStartTime());
		message.setEndTime(event.getEndTime());
		message.setDurationInMillis(event.getDurationInMillis());
	}

	private static TaskEventMessage toTaskEventMessage(HistoricTaskInstanceEventEntity event) {
		TaskEventMessage task = new TaskEventMessage();
		task.setId(event.getId());
		task.setAssignee(event.getAssignee());
		task.setDeleteReason(event.getDeleteReason());
		task.setDurationInMillis(event.getDurationInMillis());
		task.setEndTime(event.getEndTime());
		task.setEventType(event.getEventType());
		task.setExecutionId(event.getExecutionId());
		task.setName(event.getName());
		task.setSequenceCounter(event.getSequenceCounter());
		task.setStartTime(event.getStartTime());
		task.setTaskDefinitionKey(event.getTaskDefinitionKey());
		return task;
	}

	private static Object extractValue(HistoricVariableUpdateEventEntity event) {
		if (event.getLongValue() != null) {
			return event.getLongValue();
		}
		if (event.getDoubleValue() != null) {
			return event.getDoubleValue();
		}
		if (event.getTextValue2() != null) {
			return event.getTextValue2();
		}
		if (event.getTextValue() != null) {
			return event.getTextValue();
		}
		// Variáveis serializadas em byte array (arquivos, objetos) não têm uma representação
		// textual direta aqui; ficam de fora da mensagem em vez de expor o id interno do blob.
		return null;
	}
}
