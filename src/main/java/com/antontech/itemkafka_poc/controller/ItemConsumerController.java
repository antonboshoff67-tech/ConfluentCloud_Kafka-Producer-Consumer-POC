package com.antontech.itemkafka_poc.controller;

import com.antontech.itemkafka_poc.kafka.consumer.ItemConsumerService;
import com.antontech.itemkafka_poc.model.ManualConsumeRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST entry points for checking and manually triggering Item consumption
 * from the shared Kafka topic. See {@code API_DOCUMENTATION.md} for curl examples.
 */
@Slf4j
@RestController
@RequestMapping(path = "item-kafka/consumer/")
@Tag(name = "Item Kafka Consumer API", description = "The Item Kafka Consumer API")
public class ItemConsumerController {

    @Autowired
    private ItemConsumerService itemConsumerService;

    private static final String ITEM_AUTO_GROUP = "item_group";
    private static final String ITEM_MANUAL_GROUP = "manual-item-group";
    private static final String INCORRECT_CONSUMER_GROUP_PASSED = "Incorrect consumer group. Use item_group or manual-item-group.";

    /**
     * @return a plain-text message indicating whether a continuous consumer is currently running.
     */
    @Operation(summary = "Check the current status of item consumption", description = "Checks if the item consumer is actively running")
    @GetMapping(path = "consume-status/v1", produces = "application/json")
    public String checkConsumerStatus() {
        return itemConsumerService.isRunning() ? "Consumer is actively listening." : "Consumer is not running.";
    }

    /**
     * Triggers a one-off, time-boxed poll of the shared Item topic using the
     * requested consumer group.
     *
     * @param request the group id ({@code item_group} or {@code manual-item-group}) and an optional message.
     * @return a summary of how many items were consumed, or a validation error message.
     */
    @Operation(summary = "Manually consume an item from the Kafka topic", description = "Triggers manual consumption of an item from the Kafka topic with a specified group ID.")
    @PostMapping(path = "manual-consume/v1", consumes = "application/json", produces = "application/text")
    public String manualConsumeItem(@RequestBody ManualConsumeRequest request) {
        if (request.getGroupId().equalsIgnoreCase(ITEM_AUTO_GROUP) || request.getGroupId().equalsIgnoreCase(ITEM_MANUAL_GROUP)) {
            return itemConsumerService.manualConsume(request.getGroupId());
        }
        return INCORRECT_CONSUMER_GROUP_PASSED;
    }
}


