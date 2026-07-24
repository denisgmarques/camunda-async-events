package br.com.acme.camunda_async_events.delegate;

import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Substitui {@link ViaCepDelegate} quando o profile {@code loadtest} está ativo (ver
 * {@code loadtest/stress-test.js}): mesmo bean name, mesma limpeza de CEP, mas sem chamar a
 * API real do ViaCEP pela rede — gerar carga contra um serviço público de terceiros derrubaria
 * o teste em rate limit bem antes de revelar qualquer gargalo desta aplicação.
 */
@Profile("loadtest")
@Component("viaCepDelegate")
public class LoadTestViaCepDelegate implements JavaDelegate {

	@Override
	public void execute(DelegateExecution execution) {
		String cepInformado = (String) execution.getVariable("cep");
		String cepLimpo = cepInformado == null ? "" : cepInformado.replaceAll("\\D", "");
		execution.setVariable("cep", cepLimpo);

		execution.setVariable("endereco_encontrado", true);
		execution.setVariable("rua", "Rua de Teste de Carga");
		execution.setVariable("bairro", "Bairro de Teste de Carga");
		execution.setVariable("cidade", "Cidade de Teste de Carga");
		execution.setVariable("uf", "TC");
		execution.setVariable("complemento", "");
	}
}
