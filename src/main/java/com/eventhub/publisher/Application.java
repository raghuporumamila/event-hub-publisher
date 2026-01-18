package com.eventhub.publisher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class Application {
	public static void main(String[] args) {
			//SpringApplication.run(Application.class, args);
				SpringApplication app = new SpringApplication(Application.class);

        app.setDefaultProperties(Collections
                .singletonMap("server.port", "8083"));
              app.run(args);
    }

	@Bean
	public RestTemplate getRestTemplate() {
		return new RestTemplate();
	}
}
