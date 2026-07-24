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
 *   <b>as entidades que a própria transação acabou de gravar</b> (recebidas em memória, sem
 *   SELECT nenhum) em vez de consultar o banco por tudo que ainda está pendente. Isso é o que
 *   garante que uma JVM nunca mexe em linhas escritas por outra transação/instância no caminho
 *   comum (broker saudável): cada instância só publica o que ela mesma produziu, ninguém disputa
 *   a mesma linha, e nem precisa reler o que acabou de escrever.</li>
 *   <li>{@link #relayPendingMessages()} — rede de segurança agendada, varre TODAS as linhas que
 *   ainda existem na tabela (de qualquer origem — não há mais coluna de status: a linha some
 *   assim que é confirmada, então "existir" já é o único estado pendente). Precisa ser ampla
 *   assim de propósito: é o único jeito de uma instância sobrevivente resgatar uma linha que
 *   outra JVM escreveu e não chegou a publicar antes de cair (crash entre o insert e o disparo
 *   assíncrono, ou o broker fora do ar por tempo suficiente). Restringir esse caminho também
 *   por origem devolveria a garantia de "sem corrida", mas quebraria a recuperação — a linha
 *   órfã nunca mais seria reenviada por ninguém.</li>
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
		List<OutboxMessage> pending = repository.findAllByOrderById();
		if (pending.isEmpty()) {
			return;
		}

		log.debug("Relay encontrou {} mensagem(ns) outbox pendente(s)", pending.size());
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
	public synchronized void triggerAsync(List<OutboxMessage> outboxMessages) {
		if (outboxMessages.isEmpty()) {
			return;
		}

		outboxMessages.forEach(publisher::publish);
	}
}
