package br.com.acme.camunda_async_events.delegate;

import org.camunda.bpm.engine.delegate.BpmnError;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Ativo em todo profile, exceto {@code loadtest} (ver {@link LoadTestViaCepDelegate}) — um teste
 * de carga martelando a API real do ViaCEP se auto-sabota em rate limit em segundos.
 */
@Profile("!loadtest")
@Component("viaCepDelegate")
public class ViaCepDelegate implements JavaDelegate {

	public static final String ERROR_CODE_VIACEP_INDISPONIVEL = "VIACEP_INDISPONIVEL";

	private final RestClient restClient = RestClient.create("https://viacep.com.br/ws");

	@Override
	public void execute(DelegateExecution execution) {
		String cepInformado = (String) execution.getVariable("cep");
		String cepLimpo = cepInformado == null ? "" : cepInformado.replaceAll("\\D", "");
		execution.setVariable("cep", cepLimpo);

		if (cepLimpo.length() != 8) {
			// CEP mal formado: entrada inválida do usuário, não é falha técnica do ViaCEP.
			execution.setVariable("endereco_encontrado", false);
			return;
		}

		ViaCepResponse endereco = consultar(cepLimpo);

		if (endereco == null || Boolean.TRUE.equals(endereco.erro())) {
			execution.setVariable("endereco_encontrado", false);
			return;
		}

		execution.setVariable("endereco_encontrado", true);
		execution.setVariable("rua", endereco.logradouro());
		execution.setVariable("bairro", endereco.bairro());
		execution.setVariable("cidade", endereco.localidade());
		execution.setVariable("uf", endereco.uf());
		execution.setVariable("complemento", endereco.complemento());
	}

	private ViaCepResponse consultar(String cepLimpo) {
		try {
			return restClient.get()
					.uri("/{cep}/json/", cepLimpo)
					.retrieve()
					.body(ViaCepResponse.class);
		}
		catch (RestClientException e) {
			// Falha técnica (timeout, conexão recusada, 5xx) - sinaliza como erro de negócio
			// do BPMN para o loop de retentativa do processo consultaCepProcess decidir se retenta.
			throw new BpmnError(ERROR_CODE_VIACEP_INDISPONIVEL, "Falha ao consultar o ViaCEP: " + e.getMessage());
		}
	}
}
