package br.com.acme.camunda_async_events.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, Long> {

	/** Não existe mais filtro por status: toda linha que existe aqui está, por definição, pendente. */
	List<OutboxMessage> findAllByOrderById();

	/**
	 * Sobrescreve {@link JpaRepository#deleteById(Object)} para ser um DELETE puro, sem SELECT
	 * antes: a implementação padrão do Spring Data faz um {@code findById()} e só então apaga a
	 * entidade carregada, o que reintroduziria a leitura que o caminho de baixa latência do
	 * {@link OutboxPublisher} foi feito pra evitar.
	 */
	@Override
	@Modifying
	@Query("DELETE FROM OutboxMessage o WHERE o.id = :id")
	void deleteById(@Param("id") Long id);
}
