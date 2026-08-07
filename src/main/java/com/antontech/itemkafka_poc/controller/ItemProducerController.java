package com.antontech.itemkafka_poc.controller;

import com.antontech.itemkafka_poc.kafka.producer.ItemProducerService;
import com.antontech.itemkafka_poc.model.Item;
import com.antontech.itemkafka_poc.repos.ItemRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;

/**
 * REST entry point for producing {@link Item} records onto Kafka, sourced
 * directly from the MS SQL Server {@code ITEM} table via {@link ItemRepository}.
 * See {@code API_DOCUMENTATION.md} for a curl example.
 */
@Slf4j
@RestController
@RequestMapping(path = "item-kafka/app/")
@Tag(name = "Item Kafka Producer and Consumer Controller", description = "The Item Kafka Producer and Consumer API")
public class ItemProducerController {

    @Autowired
    private ItemRepository itemRepository;

    @Autowired
    private ItemProducerService itemProducerService;

    /**
     * Reads up to 100 Item rows from MS SQL Server and publishes them to
     * the shared Kafka Item topic.
     *
     * @return a plain-text confirmation or error message.
     */
    @Operation(summary = "Read items and publish them to Kafka", description = "Reads the first set of items and sends them to the Kafka topic.", tags = {"msg"})
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Message sent"),
            @ApiResponse(responseCode = "400", description = "Invalid input"),
            @ApiResponse(responseCode = "500", description = "Service down")})
    @PostMapping(path = "publish-items/v1", consumes = "application/json", produces = "application/text")
    public String createItemKafkaTopic() {
        List<Item> items = itemRepository.findFirst100ByItemIdIsNotNull();
        if (items == null || items.isEmpty()) {
            items = Collections.emptyList();
            log.warn("No items found to publish");
        } else {
            log.debug("Items size = {}", items.size());
        }

        try {
            itemProducerService.sendItems(items);
        } catch (Exception e) {
            log.error("Error occurred in createItemKafkaTopic", e);
            return "Error occurred in createItemKafkaTopic!";
        }
        return "Items sent to Kafka topic successfully!";
    }
}


