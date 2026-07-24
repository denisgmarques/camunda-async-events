package br.com.acme.camunda_async_events.outbox;

import br.com.acme.camunda_async_events.rabbitmq.CamundaEventsRabbitProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Publica mensagens outbox no RabbitMQ por dois caminhos. Nenhum dos dois é {@code synchronized}
 * — de propósito, apesar de {@link OutboxRelay} ser um singleton do Spring compartilhado por toda
 * transação e pela varredura agendada:
 *
 * <ul>
 *   <li>{@link #triggerAsync(List)} — disparo de baixa latência logo após o commit, publica
 *   <b>as entidades que a própria transação acabou de gravar</b> (recebidas em memória, sem
 *   SELECT nenhum) em vez de consultar o banco por tudo que ainda está pendente. Transações
 *   diferentes chamam isso concorrentemente o tempo todo — sem problema, cada uma só enxerga e
 *   publica as próprias linhas, nunca as de outra.</li>
 *   <li>{@link #relayPendingMessages()} — rede de segurança agendada, varre as linhas que ainda
 *   existem na tabela e já passaram da idade mínima configurada em
 *   {@code camunda.events.rabbitmq.relay-min-age} (de qualquer origem — não há mais coluna de
 *   status: a linha some assim que é confirmada, então "existir" já é o único estado pendente).
 *   Precisa continuar ampla (sem filtro por origem) de propósito: é o único jeito de uma
 *   instância sobrevivente resgatar uma linha que outra JVM escreveu e não chegou a publicar
 *   antes de cair. O filtro por idade não é sobre correção, é sobre não desperdiçar ciclo:
 *   quase toda linha nova já vai ter sido publicada pelo caminho de baixa latência antes da
 *   idade mínima passar, então a varredura normalmente só encontra o que sobrou de verdade.</li>
 * </ul>
 *
 * <p>O único cenário onde os dois caminhos podem mesmo disputar a mesma linha é uma corrida bem
 * estreita: a varredura pega uma linha (já mais velha que a idade mínima, então rara — o caminho
 * rápido normalmente já teria terminado) no instante em que o disparo de baixa latência daquela
 * mesma transação ainda está processando ela. Isso <b>não</b> foi resolvido com um lock — de
 * propósito. Um {@code synchronized} aqui protegeria o publish inteiro, incluindo a espera pelo
 * publisher-confirm do RabbitMQ (até {@code publish-confirm-timeout}, alguns segundos), o que
 * serializaria toda transação atrás de qualquer outra publicação em andamento, mesmo mexendo em
 * linhas totalmente diferentes — o oposto do que "baixa latência" deveria significar. E o
 * problema que ele evitaria já é tolerado sem lock nenhum entre JVMs diferentes (ver
 * "Known limitations" no README): o consumidor é idempotente, absorve a publicação duplicada;
 * pagar o custo de serializar I/O só pra evitar a mesma duplicata dentro de uma única JVM não é
 * consistente com essa decisão. (E o delete, dos dois lados, usa
 * {@link OutboxMessageRepository#deleteById(Long)} — um {@code DELETE} sem checagem de linhas
 * afetadas — em vez do delete por entidade padrão do Spring Data, que lançaria
 * {@code OptimisticLockException} exatamente nessa corrida; ver
 * {@code OutboxMessageRepositoryDeleteRaceTest}.)
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class OutboxRelay {

	private final OutboxMessageRepository repository;
	private final OutboxPublisher publisher;
	private final CamundaEventsRabbitProperties properties;

	@Scheduled(fixedDelayString = "${camunda.events.rabbitmq.relay-interval-ms:5000}")
	public void relayPendingMessages() {
		Instant threshold = Instant.now().minus(properties.getRelayMinAge());
		List<OutboxMessage> pending = repository.findByCreatedAtBeforeOrderById(threshold);
		if (pending.isEmpty()) {
			return;
		}

		log.debug("Relay encontrou {} mensagem(ns) outbox pendente(s) com mais de {}", pending.size(),
				properties.getRelayMinAge());
		pending.forEach(message -> publisher.publish(message.getId()));
	}

	/**
	 * Disparo de baixa latência logo após o commit da transação do Camunda. Recebe as entidades
	 * exatas que a própria transação acabou de gravar (mantidas em memória, do
	 * {@code beforeCommit} até aqui, pela sincronização que as produziu) em vez de perguntar
	 * ao banco "o que está pendente" — essa é a diferença entre publicar exatamente o que é
	 * seu, sem reler nada, e disputar linha por linha com outras instâncias.
	 */
	@Async
	public void triggerAsync(List<OutboxMessage> outboxMessages) {
		if (outboxMessages.isEmpty()) {
			return;
		}

		outboxMessages.forEach(publisher::publish);
	}
}
