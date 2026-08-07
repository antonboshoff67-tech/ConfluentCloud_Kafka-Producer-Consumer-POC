package com.antontech.itemkafka_poc.service;

import com.antontech.itemkafka_poc.exceptions.ConsumerException;
import com.antontech.itemkafka_poc.model.ServiceRequest;

/** Routes inbound/outbound test messages between the REST layer and the downstream gateway HTTP endpoint. */
public interface MsgRoutingService {

  /**
   * Handles a message that was "received" from the Kafka consumer side of the test flow (logging only in this POC).
   *
   * @param serviceRequest the request payload containing the message text.
   */
  void processReceivedMsgRequest(ServiceRequest serviceRequest);

  /**
   * Builds a signed JWT, prepares HTTP headers and forwards the request towards the configured gateway endpoint.
   *
   * @param serviceRequest the request payload containing the message text.
   * @throws ConsumerException if the JWT could not be built or the downstream call failed.
   */
  void processSentMsgRequest(ServiceRequest serviceRequest) throws ConsumerException;
}

