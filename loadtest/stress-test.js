import http from 'k6/http';
import { check } from 'k6';

// Teste de estresse em rampa: sobe carga aos poucos, segura, desce - o objetivo nao e "ate
// quebrar", e ver EM QUE PONTO a latencia do publisher-confirm e o backlog do outbox comecam a
// crescer mais rapido que a sweep consegue drenar (acompanhe ao vivo no Grafana, localhost:3000).
//
// Roda contra o profile "loadtest" (troca o ViaCepDelegate real por um fake, ver
// LoadTestViaCepDelegate) - NAO aponte isso para uma instancia usando o ViaCepDelegate real, ela
// vai bater rate limit no ViaCEP em segundos e os numeros deixam de significar algo sobre esta
// aplicacao.
//
// Uso:
//   ./mvnw spring-boot:run -Dspring-boot.run.profiles=loadtest
//   k6 run loadtest/stress-test.js
//   (opcional) k6 run --env BASE_URL=http://localhost:8080 loadtest/stress-test.js

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export const options = {
	stages: [
		{ duration: '30s', target: 10 },  // rampa: 0 -> 10 usuarios virtuais
		{ duration: '1m', target: 10 },   // segura em 10
		{ duration: '30s', target: 50 },  // rampa: 10 -> 50
		{ duration: '2m', target: 50 },   // segura em 50 - normalmente onde o gargalo aparece
		{ duration: '30s', target: 0 },   // rampa de volta pra 0
	],
	thresholds: {
		// Sinal de aprovado/reprovado no final, nao trava o teste no meio.
		http_req_failed: ['rate<0.01'],
		http_req_duration: ['p(95)<2000'],
	},
};

export default function () {
	const payload = JSON.stringify({
		variables: {
			nome: { value: `Teste de Carga ${__VU}-${__ITER}`, type: 'String' },
			cpf: { value: '999.888.777-66', type: 'String' },
			cep: { value: '01001-000', type: 'String' },
		},
	});

	const res = http.post(
		`${BASE_URL}/engine-rest/process-definition/key/cadastroClienteProcess/start`,
		payload,
		{ headers: { 'Content-Type': 'application/json' } },
	);

	check(res, {
		// A API REST do Camunda responde 200 (nao 201) mesmo criando o recurso.
		'processo iniciado (200)': (r) => r.status === 200,
	});
}
