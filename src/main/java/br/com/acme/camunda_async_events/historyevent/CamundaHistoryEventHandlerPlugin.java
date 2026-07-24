package br.com.acme.camunda_async_events.historyevent;

import lombok.RequiredArgsConstructor;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.spring.boot.starter.configuration.CamundaProcessEngineConfiguration;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Registra o {@link CamundaHistoryEventHandler} como um handler ADICIONAL, mantendo o
 * handler padrão do Camunda ativo (as tabelas ACT_HI_* continuam sendo escritas
 * normalmente, então Cockpit/Tasklist não perdem histórico). Usar
 * {@code setHistoryEventHandler} no lugar de {@code customHistoryEventHandlers}
 * substituiria o handler padrão inteiro — não é isso que queremos aqui.
 */
@Component
@RequiredArgsConstructor
public class CamundaHistoryEventHandlerPlugin implements CamundaProcessEngineConfiguration {

	private final CamundaHistoryEventHandler historyEventHandler;

	@Override
	public void preInit(ProcessEngineConfigurationImpl configuration) {
		// Lista mutável: o próprio starter do Camunda (EventPublisherPlugin) adiciona o
		// handler dele nessa mesma lista logo em seguida.
		configuration.setCustomHistoryEventHandlers(new ArrayList<>(List.of(historyEventHandler)));
	}
}
