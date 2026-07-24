package br.com.acme.camunda_async_events;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableAsync
@EnableScheduling
@SpringBootApplication
public class CamundaAsyncEventsApplication {

	public static void main(String[] args) {
		SpringApplication.run(CamundaAsyncEventsApplication.class, args);
	}

}
