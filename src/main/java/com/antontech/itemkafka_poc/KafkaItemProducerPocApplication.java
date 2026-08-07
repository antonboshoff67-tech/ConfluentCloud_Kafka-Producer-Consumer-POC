package com.antontech.itemkafka_poc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot entry point for the Item Kafka Producer/Consumer + Flink POC
 * application. See {@code SETUP_GUIDE.md}, {@code API_DOCUMENTATION.md} and
 * {@code ARCHITECTURE.md} at the repository root for setup, endpoint and
 * end-to-end architecture documentation respectively.
 */
@SpringBootApplication
public class KafkaItemProducerPocApplication
{

  /** @param args standard Spring Boot command-line arguments (unused). */
  public static void main(String[] args)
  {
    SpringApplication.run(KafkaItemProducerPocApplication.class, args);
  }
}

