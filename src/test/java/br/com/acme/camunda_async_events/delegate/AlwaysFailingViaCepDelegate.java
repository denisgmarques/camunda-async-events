package br.com.acme.camunda_async_events.delegate;

import org.camunda.bpm.engine.delegate.BpmnError;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;

/**
 * Simula o ViaCEP tecnicamente indisponível em toda chamada (timeout/conexão recusada),
 * para testar a "teimosinha" de retentativa do {@code consultaCepProcess}.
 */
public class AlwaysFailingViaCepDelegate implements JavaDelegate {

	@Override
	public void execute(DelegateExecution execution) {
		throw new BpmnError(ViaCepDelegate.ERROR_CODE_VIACEP_INDISPONIVEL, "Falha simulada para teste");
	}
}
