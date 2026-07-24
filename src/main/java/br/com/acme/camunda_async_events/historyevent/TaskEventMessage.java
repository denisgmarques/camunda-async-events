package br.com.acme.camunda_async_events.historyevent;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
public class TaskEventMessage {

	private String id;
	private String assignee;
	private String deleteReason;
	private String taskDefinitionKey;
	private String name;
	private Long durationInMillis;
	private Date startTime;
	private Date endTime;
	private String executionId;
	private String eventType;
	private long sequenceCounter;
}
