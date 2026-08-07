package com.antontech.itemkafka_poc.controller;

import com.antontech.itemkafka_poc.model.ServiceRequest;
import com.antontech.itemkafka_poc.service.MsgRoutingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST entry points that exercise the JWT-signed gateway message routing test
 * flow via {@link MsgRoutingService}. See {@code API_DOCUMENTATION.md} for
 * curl examples.
 */
@Slf4j
@RestController
@RequestMapping(path = "item-kafka/app/")
@Tag(name = "Item Kafka Producer POC", description = "The Item Kafka Consumer Msg Test API")
public class MsgConsumerController {

  @Autowired
  private MsgRoutingService msgRoutingService;

  /**
   * Builds a signed JWT and prepares a forwarding request to the configured
   * gateway endpoint for the supplied message.
   *
   * @param serviceRequest the message payload to forward.
   * @return HTTP 200 on success, HTTP 400 with the failure reason otherwise.
   */
  @Operation(summary = "Sending a message to the Kafka producer", description = "New message sent", tags = {"msg"})
  @ApiResponses(value = {
      @ApiResponse(responseCode = "200", description = "Message sent"),
      @ApiResponse(responseCode = "400", description = "Invalid input"),
      @ApiResponse(responseCode = "500", description = "Service down")})
  @PostMapping(path = "send-items/v1", consumes = "application/json", produces = "application/text")
  public ResponseEntity<Object> sendItemsToKafka(@RequestBody final ServiceRequest serviceRequest) {
    try {
      msgRoutingService.processSentMsgRequest(serviceRequest);
      return new ResponseEntity<>("The items were prepared for publishing to the Kafka topic.", HttpStatus.OK);
    } catch (Exception e) {
      log.error("Failed to prepare items for Kafka", e);
      return new ResponseEntity<>("There was a problem: " + e.getMessage(), HttpStatus.BAD_REQUEST);
    }
  }

  /**
   * Simulates handling of a message "received" from the Kafka consumer side
   * of the test flow (logs the message only).
   *
   * @param authToken      optional Bearer token, logged if present.
   * @param serviceRequest the message payload received.
   * @return HTTP 200 on success, HTTP 400 with the failure reason otherwise.
   */
  @Operation(summary = "Receiving messages from the Kafka consumer", description = "New messages received", tags = {"msg"})
  @ApiResponses(value = {
      @ApiResponse(responseCode = "200", description = "Message received"),
      @ApiResponse(responseCode = "400", description = "Invalid input"),
      @ApiResponse(responseCode = "500", description = "Service down")})
  @GetMapping(path = "consume-items/v1", consumes = "application/json", produces = "application/text")
  public ResponseEntity<Object> consumeItemsFromKafka(@RequestHeader(name = "Authorization", required = false) final String authToken,
                                                       @RequestBody final ServiceRequest serviceRequest) {
    if (authToken != null && !authToken.isBlank()) {
      log.debug("Authorization header received for consume request");
    }
    try {
      msgRoutingService.processReceivedMsgRequest(serviceRequest);
      return new ResponseEntity<>("Message items were processed successfully from the Kafka consumer.", HttpStatus.OK);
    } catch (Exception e) {
      log.error("Failed to process consumed items", e);
      return new ResponseEntity<>("There was a problem: " + e.getMessage(), HttpStatus.BAD_REQUEST);
    }
  }
}


