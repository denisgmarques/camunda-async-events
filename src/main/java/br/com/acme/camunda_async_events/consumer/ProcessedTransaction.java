package br.com.acme.camunda_async_events.consumer;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Marca que uma mensagem já foi processada por este consumidor — a chave primária
 * ({@link ProcessedTransactionId}) é o que garante idempotência mesmo sob reentregas
 * concorrentes: uma segunda tentativa de gravar o mesmo id esbarra na constraint e é
 * tratada como duplicata.
 */
@Entity
@Table(name = "processed_transaction")
@Getter
@NoArgsConstructor
public class ProcessedTransaction {

	@EmbeddedId
	private ProcessedTransactionId id;

	@Column(name = "processed_at", nullable = false)
	private Instant processedAt = Instant.now();

	public ProcessedTransaction(ProcessedTransactionId id) {
		this.id = id;
	}
}
