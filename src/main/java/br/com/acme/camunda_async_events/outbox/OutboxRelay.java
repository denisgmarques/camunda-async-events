package br.com.acme.camunda_async_events.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Publica mensagens outbox no RabbitMQ por dois caminhos, ambos serializados pelo mesmo
 * monitor (os dois métodos são {@code synchronized} na mesma instância) para nunca rodar ao
 * mesmo tempo dentro desta JVM:
 *
 * <ul>
 *   <li>{@link #triggerAsync(List)} — disparo de baixa latência logo após o commit, publica
 *   <b>só os ids que a própria transação acabou de escrever</b>, sem consultar o banco por
 *   tudo que está PENDING. Isso é o que garante que uma JVM nunca mexe em linhas escritas por
 *   outra transação/instância no caminho comum (broker saudável): cada instância só publica o
 *   que ela mesma produziu, ninguém disputa a mesma linha.</li>
 *   <li>{@link #relayPendingMessages()} — rede de segurança agendada, varre TODAS as linhas
 *   PENDING (de qualquer origem). Precisa ser ampla assim de propósito: é o único jeito de uma
 *   instância sobrevivente resgatar uma linha que outra JVM escreveu e não chegou a publicar
 *   antes de cair (crash entre o insert e o disparo assíncrono, ou o broker fora do ar por
 *   tempo suficiente). Restringir esse caminho também por origem devolveria a garantia de
 *   "sem corrida", mas quebraria a recuperação — a linha órfã nunca mais seria reenviada por
 *   ninguém.</li>
 * </ul>
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class OutboxRelay {

	private final OutboxMessageRepository repository;
	private final OutboxPublisher publisher;

	@Scheduled(fixedDelayString = "${camunda.events.rabbitmq.relay-interval-ms:5000}")
	public synchronized void relayPendingMessages() {
		List<OutboxMessage> pending = repository.findByStatusOrderById(OutboxStatus.PENDING);
		if (pending.isEmpty()) {
			return;
		}

		log.debug("Relay encontrou {} mensagem(ns) outbox pendente(s)", pending.size());
		pending.forEach(message -> publisher.publish(message.getId()));
	}

	/**
	 * Disparo de baixa latência logo após o commit da transação do Camunda. Recebe os ids
	 * exatos das linhas que a própria transação acabou de gravar (mantidos em memória, do
	 * {@code beforeCommit} até aqui, pela sincronização que os produziu) em vez de perguntar
	 * ao banco "o que está pendente" — essa é a diferença entre publicar exatamente o que é
	 * seu e disputar linha por linha com outras instâncias.
	 */
	@Async
	public synchronized void triggerAsync(List<Long> outboxMessageIds) {
		if (outboxMessageIds.isEmpty()) {
			return;
		}

		outboxMessageIds.forEach(publisher::publish);
	}
}
