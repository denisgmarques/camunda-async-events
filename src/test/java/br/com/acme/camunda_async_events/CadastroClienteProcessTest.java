package br.com.acme.camunda_async_events;

import br.com.acme.camunda_async_events.delegate.AlwaysFailingViaCepDelegate;
import br.com.acme.camunda_async_events.delegate.FakeViaCepDelegate;
import org.camunda.bpm.engine.history.HistoricProcessInstance;
import org.camunda.bpm.engine.runtime.Job;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.task.Task;
import org.camunda.bpm.engine.test.Deployment;
import org.camunda.bpm.engine.test.mock.Mocks;
import org.camunda.community.process_test_coverage.junit5.platform7.ProcessEngineCoverageExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static org.camunda.bpm.engine.test.assertions.bpmn.BpmnAwareTests.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(ProcessEngineCoverageExtension.class)
@Deployment(resources = { "bpmn/cadastro-cliente.bpmn", "bpmn/consulta-cep.bpmn" })
class CadastroClienteProcessTest {

	private static final String PROCESS_DEFINITION_KEY = "cadastroClienteProcess";

	@BeforeEach
	void mockingDelegates() {
		Mocks.register("viaCepDelegate", new FakeViaCepDelegate());
	}

	@AfterEach
	void clearMocks() {
		Mocks.reset();
	}

	@Test
	void shouldStartWithNomeCpfECepAndWaitOnViaCepServiceTask() {
		ProcessInstance processInstance = startCadastro("Ana Lima", "111.222.333-44", "40010-000");

		assertEquals("Ana Lima", runtimeService().getVariable(processInstance.getId(), "nome"));
		assertEquals("111.222.333-44", runtimeService().getVariable(processInstance.getId(), "cpf"));
		assertEquals("40010-000", runtimeService().getVariable(processInstance.getId(), "cep"));
		assertThat(processInstance).isWaitingAt("CallActivity_ConsultarCep");
	}

	@Test
	void shouldExecuteHappyPath() {
		ProcessInstance processInstance = startCadastro("Maria Silva", "123.456.789-00", "01001-000");

		executePendingJob(processInstance);
		Task avaliarCadastro = getTask(processInstance, "Task_AvaliarCadastro");
		assertNotNull(avaliarCadastro, "Esperava a tarefa 'Avaliar Cadastro' ativa");
		assertEquals("01001000", runtimeService().getVariable(processInstance.getId(), "cep"));
		assertEquals("Rua de Teste", runtimeService().getVariable(processInstance.getId(), "rua"));

		completeTask(avaliarCadastro, Map.of("cadastro_ok", true));

		assertProcessInstanceEnded(processInstance);
	}

	@Test
	void shouldGoThroughCorrectionLoopWhenCadastroIsNotOk() {
		ProcessInstance processInstance = startCadastro("Joao Souza", "987.654.321-00", "20040-020");

		executePendingJob(processInstance);
		Task avaliarCadastro = getTask(processInstance, "Task_AvaliarCadastro");
		completeTask(avaliarCadastro, Map.of("cadastro_ok", false));

		Task corrigirDados = getTask(processInstance, "Task_CorrigirDados");
		assertNotNull(corrigirDados, "Esperava a tarefa 'Corrigir Dados' ativa");
		completeTask(corrigirDados, Map.of("cep", "70150-900"));

		// ao sair de "Corrigir Dados" o fluxo passa de novo pela CallActivity do ViaCEP
		assertThat(processInstance).isWaitingAt("CallActivity_ConsultarCep");
		executePendingJob(processInstance);

		Task avaliarCadastroNovamente = getTask(processInstance, "Task_AvaliarCadastro");
		assertNotNull(avaliarCadastroNovamente, "Esperava voltar para 'Avaliar Cadastro' apos a correcao");
		assertEquals("70150900", runtimeService().getVariable(processInstance.getId(), "cep"));

		completeTask(avaliarCadastroNovamente, Map.of("cadastro_ok", true));

		assertProcessInstanceEnded(processInstance);
	}

	@Test
	void shouldRetryFiveTimesThenGiveUpGracefullyWhenViaCepIsPersistentlyDown() {
		Mocks.register("viaCepDelegate", new AlwaysFailingViaCepDelegate());

		ProcessInstance processInstance = startCadastro("Carlos Falha", "222.333.444-55", "01001-000");

		// 1a tentativa acontece dentro do proprio job assincrono da CallActivity
		executePendingJob(processInstance);

		// mais 3 rodadas de espera (timer PT15S) + nova tentativa = tentativas 2, 3 e 4
		for (int retry = 1; retry <= 3; retry++) {
			executeAnyPendingJob();
		}
		assertNotNull(managementService().createJobQuery().singleResult(),
				"Apos 4 tentativas o loop de retentativa ainda deveria estar esperando o timer pra 5a tentativa");

		// 5a e ultima tentativa permitida: falha e desiste graciosamente
		executeAnyPendingJob();

		Task avaliarCadastro = getTask(processInstance, "Task_AvaliarCadastro");
		assertNotNull(avaliarCadastro, "Esperava que o loop de retentativa desistisse apos 5 tentativas e voltasse para Avaliar Cadastro");
		assertEquals(Boolean.FALSE, runtimeService().getVariable(processInstance.getId(), "endereco_encontrado"));

		completeTask(avaliarCadastro, Map.of("cadastro_ok", false));
		Task corrigirDados = getTask(processInstance, "Task_CorrigirDados");
		assertNotNull(corrigirDados, "Mesmo sem endereco, o cadastro segue normalmente para correcao");
	}

	private ProcessInstance startCadastro(String nome, String cpf, String cep) {
		Map<String, Object> variables = new HashMap<>();
		variables.put("nome", nome);
		variables.put("cpf", cpf);
		variables.put("cep", cep);
		return runtimeService().startProcessInstanceByKey(PROCESS_DEFINITION_KEY, variables);
	}

	private void executePendingJob(ProcessInstance processInstance) {
		Job job = managementService().createJobQuery().processInstanceId(processInstance.getId()).singleResult();
		assertNotNull(job, "Esperava um job pendente para a ServiceTask 'Consultar ViaCEP'");
		managementService().executeJob(job.getId());
	}

	/**
	 * Executa o único job pendente no engine, seja ele da CallActivity ou de um timer de
	 * retentativa dentro do {@code consultaCepProcess} (cuja processInstanceId é a da
	 * instância filha, não a do processo principal).
	 */
	private void executeAnyPendingJob() {
		Job job = managementService().createJobQuery().singleResult();
		assertNotNull(job, "Esperava um job pendente (timer de retentativa ou CallActivity)");
		managementService().executeJob(job.getId());
	}

	private Task getTask(ProcessInstance processInstance, String taskDefinitionKey) {
		return taskService().createTaskQuery()
				.processInstanceId(processInstance.getId())
				.taskDefinitionKey(taskDefinitionKey)
				.singleResult();
	}

	private void completeTask(Task task, Map<String, Object> variables) {
		taskService().complete(task.getId(), variables);
	}

	private void assertProcessInstanceEnded(ProcessInstance processInstance) {
		HistoricProcessInstance historicProcessInstance = historyService()
				.createHistoricProcessInstanceQuery()
				.processInstanceId(processInstance.getId())
				.singleResult();
		assertNotNull(historicProcessInstance.getEndTime());
	}
}
