package br.com.acme.camunda_async_events.delegate;

import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;

/**
 * Substitui {@link ViaCepDelegate} nos testes de processo: mesmo comportamento de
 * limpeza do CEP, mas sem chamar a API real do ViaCEP pela rede.
 */
public class FakeViaCepDelegate implements JavaDelegate {

	@Override
	public void execute(DelegateExecution execution) {
		String cepInformado = (String) execution.getVariable("cep");
		String cepLimpo = cepInformado == null ? "" : cepInformado.replaceAll("\\D", "");
		execution.setVariable("cep", cepLimpo);

		execution.setVariable("endereco_encontrado", true);
		execution.setVariable("rua", "Rua de Teste");
		execution.setVariable("bairro", "Bairro de Teste");
		execution.setVariable("cidade", "Cidade de Teste");
		execution.setVariable("uf", "TS");
		execution.setVariable("complemento", "");
	}
}
