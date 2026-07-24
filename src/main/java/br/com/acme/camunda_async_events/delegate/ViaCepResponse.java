package br.com.acme.camunda_async_events.delegate;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ViaCepResponse(
		String cep,
		String logradouro,
		String complemento,
		String bairro,
		String localidade,
		String uf,
		Boolean erro
) {
}
