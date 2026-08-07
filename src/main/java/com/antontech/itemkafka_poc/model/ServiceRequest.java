package com.antontech.itemkafka_poc.model;

import lombok.Getter;
import lombok.Setter;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

/** Request/response payload used by {@link com.antontech.itemkafka_poc.controller.MsgConsumerController}'s send/consume test endpoints. */
@Service
@Getter
@Setter
public class ServiceRequest {

  /** Free-text message body carried through the send/consume test flow. */
  private String msg;

  /** @return the current {@link #msg} value, exposed as a bean for legacy wiring purposes. */
  @Bean
  public String message() {
    return msg;
  }

}

