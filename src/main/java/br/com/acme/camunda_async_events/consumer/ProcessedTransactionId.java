package br.com.acme.camunda_async_events.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Uma transação do Camunda pode gerar mais de uma mensagem quando toca mais de uma
 * processInstanceId (ex.: processo pai + processo filho de uma CallActivity, ambos
 * capturados na mesma transação/commit — ver {@code HistoryEventTransactionSynchronization}).
 * Por isso a idempotência não pode ser só pelo {@code transactionId}: a chave real de uma
 * mensagem é o par (transactionId, processInstanceId).
 */
@Embeddable
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedTransactionId implements Serializable {

	@Column(name = "transaction_id")
	private String transactionId;

	@Column(name = "process_instance_id")
	private String processInstanceId;
}
