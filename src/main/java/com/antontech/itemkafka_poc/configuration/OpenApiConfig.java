package com.antontech.itemkafka_poc.configuration;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

/** Configures the Springdoc/OpenAPI 3 metadata shown on the Swagger UI (served at {@code /agent/swagger-ui.html}). */
@Configuration
@EnableWebMvc
public class OpenApiConfig {

  /** @return the {@link OpenAPI} bean describing this application's title and description for the generated Swagger UI. */
  @Bean
  public OpenAPI customOpenAPI() {
    return new OpenAPI()
        .components(new Components())
        .info(new Info().title("Item Kafka Producer Application API").description(
            "This is a Spring Boot RESTful Consumer Application to test Item Kafka Producer using springdoc-openapi and OpenAPI 3."));
  }
}

