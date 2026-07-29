package za.co.woolworths.itemkafka_poc.controller;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import za.co.woolworths.itemkafka_poc.kafka.consumer.ItemConsumerService;
import za.co.woolworths.itemkafka_poc.kafka.consumer.LocalDateTimeAdapter;
import za.co.woolworths.itemkafka_poc.model.Item;
import za.co.woolworths.itemkafka_poc.model.ManualConsumeRequest;

import java.time.LocalDateTime;
import java.util.Collections;

@Slf4j
@RestController
@RequestMapping(path = "item-kafka/consumer/")
@Tag(name = "Item Kafka Consumer API", description = "The Item Kafka Consumer API")
public class ItemConsumerController
{

    @Autowired
    private ItemConsumerService itemConsumerService;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory; // Add this line

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
            .create(); // Ensure LocalDateTimeAdapter is used for both serialization and deserialization

    private static final String ITEM_AUTO_GROUP = "item_group";
    private static final String ITEM_MANUAL_GROUP = "manual-item-group";

    private static final String INCORRECT_CONSUMER_GROUP_PASSED = "Incorrect consumer group! Please make sure consumer groups are item_group or manual-item-group ";

    @Operation(summary = "Check the current status of item consumption",
            description = "Checks if the item consumer is actively running")
    @GetMapping(path = "consume-status/v1", produces = "application/json")
    public String checkConsumerStatus() {
        return itemConsumerService.isRunning() ? "Consumer is actively listening." : "Consumer is not running.";
    }

    @Operation(summary = "Manually consume an item from the Kafka topic",
            description = "Triggers manual consumption of an item from the Kafka topic with a specified group ID.")
    @PostMapping(path = "manual-consume/v1", consumes = "application/json", produces = "application/text")
    public String manualConsumeItem(@RequestBody ManualConsumeRequest request)
    {
        if(request.getGroupId().equalsIgnoreCase(ITEM_AUTO_GROUP) || request.getGroupId().equalsIgnoreCase(ITEM_MANUAL_GROUP))
        {
            return itemConsumerService.manualConsume(request.getGroupId()); // Call to consume manually with groupId from request body}
        }
        else
        {
            return INCORRECT_CONSUMER_GROUP_PASSED;
        }
    }

}