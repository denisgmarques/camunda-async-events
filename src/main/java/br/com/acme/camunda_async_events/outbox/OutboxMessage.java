package br.com.acme.camunda_async_events.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Registro da tabela outbox: cada linha é uma mensagem de evento de processo que precisa
 * ser publicada no RabbitMQ. É gravada na MESMA transação/conexão do Camunda (ver
 * {@link br.com.acme.camunda_async_events.historyevent.HistoryEventTransactionSynchronization}),
 * garantindo que o evento nunca se perca mesmo que o broker esteja indisponível no momento do
 * commit: o {@link OutboxRelay} republica tudo que ainda estiver PENDING.
 */
@Entity
@Table(name = "outbox_message")
@Getter
@Setter
@NoArgsConstructor
public class OutboxMessage {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "transaction_id", nullable = false, updatable = false)
	private String transactionId;

	@Column(name = "process_instance_id", nullable = false, updatable = false)
	private String processInstanceId;

	@Column(name = "process_definition_key", nullable = false, updatable = false)
	private String processDefinitionKey;

	@Lob
	@Column(name = "payload", nullable = false, updatable = false)
	private String payload;

	@Enumerated(EnumType.STRING)
	@Column(name = "status", nullable = false)
	private OutboxStatus status = OutboxStatus.PENDING;

	@Column(name = "created_at", nullable = false, updatable = false)
	private Instant createdAt = Instant.now();

	public OutboxMessage(String transactionId, String processInstanceId, String processDefinitionKey,
			String payload) {
		this.transactionId = transactionId;
		this.processInstanceId = processInstanceId;
		this.processDefinitionKey = processDefinitionKey;
		this.payload = payload;
	}
}
