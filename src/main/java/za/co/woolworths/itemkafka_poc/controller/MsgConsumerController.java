package za.co.woolworths.itemkafka_poc.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import lombok.extern.slf4j.Slf4j;
import za.co.woolworths.itemkafka_poc.model.ServiceRequest;
import za.co.woolworths.itemkafka_poc.service.MsgRoutingService;

// testing endpoint to hit this controller and method to send items to Kafka topic - http://localhost:8081/item-kafka/app/send-items/v1

@Slf4j
@RestController
@RequestMapping(path = "item-kafka/app/")
@Tag(name = "Item Kafka Producer POC", description = "The Item Kafka Consumer Msg Test API")
public class MsgConsumerController
{

  @Autowired
  private MsgRoutingService msgRoutingService;

  @Operation(summary = "Sending a Message to the Kafka Producer.", description = "New MSG Sent", tags = { "msg" })
  @ApiResponses(value = {
      @ApiResponse(responseCode = "200", description = "Message Sent"),
      @ApiResponse(responseCode = "400", description = "Invalid Input"),
      @ApiResponse(responseCode = "500", description = "Service Down")})
  @PostMapping(path = "send-items/v1", consumes = "application/json", produces = "application/text")
  public ResponseEntity<Object> sendItemsToKafka(@RequestBody final ServiceRequest serviceRequest) {
    log.debug("Create-Message ...");
    ResponseEntity<Object> responseEntity;
    try
    {
      msgRoutingService.processSentMsgRequest(serviceRequest);
      responseEntity = new ResponseEntity<>("The items were successfully processed from the DB and send to the Kafka topic.", HttpStatus.OK);
    }
    catch (Exception e)
    {
      responseEntity = new ResponseEntity<>("There was a problem : " + e.getMessage(), HttpStatus.BAD_REQUEST);
    }
    return responseEntity;
  }

  @Operation(summary = "Receiving Messages from the Kafka Consumer.", description = "New MSG's Received", tags = { "msg" })
  @ApiResponses(value = {
      @ApiResponse(responseCode = "200", description = "Message Received"),
      @ApiResponse(responseCode = "400", description = "Invalid Input"),
      @ApiResponse(responseCode = "500", description = "Service Down")})
  @GetMapping(path = "consume-items/v1", consumes = "application/json", produces = "application/text")
  public ResponseEntity<Object> consumeItemsFromKafka(@RequestHeader(name = "Authorization", required = false) final String authToken, @RequestBody final ServiceRequest serviceRequest)
  {
    log.debug("Message Items processed from Kafka Consumer and successfully to be written to the MySql DB ...");
    ResponseEntity<Object> responseEntity;
    try {
      msgRoutingService.processReceivedMsgRequest(serviceRequest);
      responseEntity = new ResponseEntity<>("Message Items were processed successfully from Kafka Consumer and written to MySql DB.", HttpStatus.OK);
    } catch (Exception e) {
      responseEntity = new ResponseEntity<>("There was a problem : " + e.getMessage(), HttpStatus.BAD_REQUEST);
    }
    return responseEntity;
  }
}
