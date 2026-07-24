package br.com.acme.camunda_async_events.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, Long> {

	List<OutboxMessage> findByStatusOrderById(OutboxStatus status);

	/**
	 * UPDATE direto por id, sem SELECT antes: quem chama aqui já tem a entidade (possivelmente
	 * detached, vinda de fora desta transação) e só quer gravar o novo status, sem pagar o
	 * custo de um {@code merge()} (que faria um SELECT escondido pra carregar o estado atual
	 * antes de decidir o UPDATE).
	 */
	@Modifying
	@Query("UPDATE OutboxMessage o SET o.status = :status WHERE o.id = :id")
	void updateStatus(@Param("id") Long id, @Param("status") OutboxStatus status);
}
