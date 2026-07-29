package za.co.woolworths.itemkafka_poc.model;

import lombok.Getter;
import lombok.Setter;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

@Service
@Getter
@Setter
public class ServiceRequest {

  private String msg;

  @Bean
  public String message() {
    return msg;
  }

}
