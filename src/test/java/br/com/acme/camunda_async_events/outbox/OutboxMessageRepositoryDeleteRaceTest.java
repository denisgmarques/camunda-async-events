package br.com.acme.camunda_async_events.outbox;

import jakarta.persistence.EntityManager;
import jakarta.persistence.OptimisticLockException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifica, de fato, a suposição usada para tirar o {@code synchronized} de {@link OutboxRelay}:
 * que apagar (ou tentar apagar) uma linha que outra transação já apagou é um no-op silencioso,
 * não uma exceção. Sem essa garantia, a corrida entre o caminho de baixa latência e a varredura
 * agendada (ambos podem tentar publicar/apagar a mesma linha recém-commitada) quebraria em vez
 * de só desperdiçar uma publicação duplicada.
 *
 * <p>Essa suposição só é verdadeira para o {@code deleteById} sobrescrito (um {@code DELETE} em
 * JPQL puro). O delete por entidade padrão do Spring Data ({@code repository.delete(entity)})
 * <b>lança</b> {@link OptimisticLockException} na mesma situação — foi assim que a gente
 * descobriu, na prática, que {@link OutboxPublisher#publish(Long)} precisava trocar de um pro
 * outro. O segundo teste aqui documenta esse comportamento de propósito, pra ninguém "simplificar"
 * de volta pro delete por entidade sem saber por que isso quebra.
 */
@DataJpaTest
class OutboxMessageRepositoryDeleteRaceTest {

	@Autowired
	private OutboxMessageRepository repository;

	@Autowired
	private EntityManager entityManager;

	@Test
	void bulkDeleteByIdOnAnAlreadyDeletedRowIsANoOp() {
		OutboxMessage saved = repository.save(new OutboxMessage("tx-1", "pi-1", "pdk", "{}"));
		entityManager.flush();

		repository.deleteById(saved.getId());
		entityManager.flush();

		assertThatCode(() -> {
			repository.deleteById(saved.getId());
			entityManager.flush();
		}).as("segundo deleteById na mesma linha, ja apagada, nao deveria lancar nada")
				.doesNotThrowAnyException();
	}

	@Test
	void entityBasedDeleteThrowsWhenAnotherTransactionAlreadyDeletedTheRow() {
		OutboxMessage saved = repository.save(new OutboxMessage("tx-2", "pi-2", "pdk", "{}"));
		entityManager.flush();

		// Simula o caminho da sweep: OutboxPublisher.publish(Long) faz um findById e guarda a
		// entidade gerenciada, ANTES de gastar tempo publicando no RabbitMQ.
		OutboxMessage loaded = repository.findById(saved.getId()).orElseThrow();

		// Enquanto isso, "outra transacao" (o caminho de baixa latencia) ja publicou e apagou a
		// mesma linha - um DELETE em bulk simula isso sem tocar no cache de 1o nivel desta
		// entidade, exatamente como uma transacao REQUIRES_NEW separada faria.
		entityManager.createQuery("DELETE FROM OutboxMessage o WHERE o.id = :id")
				.setParameter("id", saved.getId())
				.executeUpdate();
		entityManager.flush();

		// repository.delete(entity) confere a contagem de linhas afetadas e lanca excecao se a
		// linha ja nao existir mais - por isso OutboxPublisher.publish(Long) usa deleteById, nao
		// isso. Se essa asserção algum dia parar de lançar, o delete por entidade mudou de
		// comportamento (ou o teste está desatualizado) - não é seguro trocar de volta sem
		// reconferir isso.
		assertThatThrownBy(() -> {
			repository.delete(loaded);
			entityManager.flush();
		}).isInstanceOf(OptimisticLockException.class);
	}
}
