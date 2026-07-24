#!/usr/bin/env bash
# Orquestra uma rodada completa do teste de carga: sobe RabbitMQ + Prometheus + Grafana,
# zera a fila principal (pra nao misturar backlog de uma rodada anterior), sobe a aplicacao
# no profile "loadtest" (ViaCEP fake, ver LoadTestViaCepDelegate), roda o k6 e derruba a
# aplicacao no final (Ctrl+C tambem funciona no meio - o cleanup roda do mesmo jeito).
#
# Uso:
#   ./loadtest/run.sh
#
# Ver docs/load-test-report.md pro relato de uma rodada real, e o README (secao "Monitoring
# and load testing") pro que cada metrica do dashboard significa.
set -euo pipefail

cd "$(dirname "$0")/.."

APP_LOG="$(mktemp -t camunda-loadtest-app.XXXXXX.log)"
APP_PID=""

cleanup() {
	echo
	echo "== Encerrando a aplicacao =="
	if [ -n "$APP_PID" ]; then
		kill "$APP_PID" 2>/dev/null || true
	fi
	# O mvnw spring-boot:run acima e só o processo do Maven - o JVM de verdade da aplicação é
	# um processo filho separado que "kill $APP_PID" sozinho não derruba. Mata pelo nome da
	# classe principal também, senão sobra um processo "fantasma" segurando a porta 8080 e o
	# consumo do RabbitMQ pra próxima rodada.
	pkill -f "camunda_async_events.CamundaAsyncEventsApplication" 2>/dev/null || true
	echo "Log da aplicacao ficou em $APP_LOG"
}
trap cleanup EXIT INT TERM

if ! command -v k6 >/dev/null 2>&1; then
	echo "k6 nao encontrado no PATH. Instale antes: https://k6.io/docs/get-started/installation/"
	exit 1
fi

echo "== Subindo RabbitMQ + Prometheus + Grafana =="
docker compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d

echo "== Esperando o RabbitMQ ficar pronto =="
until docker exec camunda-async-events-rabbitmq rabbitmq-diagnostics -q ping >/dev/null 2>&1; do
	sleep 1
done

echo "== Zerando camunda.events.queue pra uma rodada limpa =="
docker exec camunda-async-events-rabbitmq rabbitmqctl purge_queue camunda.events.queue >/dev/null 2>&1 || true

echo "== Subindo a aplicacao (profile loadtest - ViaCEP fake, tudo mais real) =="
./mvnw spring-boot:run -Dspring-boot.run.profiles=loadtest > "$APP_LOG" 2>&1 &
APP_PID=$!

echo "Aguardando a aplicacao subir (log completo em $APP_LOG)..."
until grep -q "Started CamundaAsyncEventsApplication" "$APP_LOG" 2>/dev/null; do
	if grep -qi "APPLICATION FAILED TO START" "$APP_LOG" 2>/dev/null; then
		echo "A aplicacao falhou ao subir - veja $APP_LOG"
		exit 1
	fi
	if ! kill -0 "$APP_PID" 2>/dev/null; then
		echo "O processo da aplicacao morreu antes de subir - veja $APP_LOG"
		exit 1
	fi
	sleep 2
done
echo "Aplicacao no ar."

echo "== Rodando o k6 (loadtest/stress-test.js, ~4m30s de rampa) =="
k6 run "$(dirname "$0")/stress-test.js"

echo
echo "== Corrida terminada =="
echo "Grafana:    http://localhost:3000 (dashboard 'Camunda Async Events - Outbox Overview')"
echo "Prometheus: http://localhost:9090"
echo "RabbitMQ:   http://localhost:15672 (camunda/camunda)"
echo
echo "Fila principal ao final:"
docker exec camunda-async-events-rabbitmq rabbitmqctl list_queues name messages consumers 2>&1 || true
