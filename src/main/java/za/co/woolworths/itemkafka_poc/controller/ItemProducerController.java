package za.co.woolworths.itemkafka_poc.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import za.co.woolworths.itemkafka_poc.model.Item;
import za.co.woolworths.itemkafka_poc.repos.ItemRepository;
import za.co.woolworths.itemkafka_poc.kafka.producer.ItemProducerService;

import java.util.List;

@Slf4j
@RestController
@RequestMapping(path = "item-kafka/app/")
@Tag(name = "Item Kafka Producer and Consumer Controller", description = "The Item Kafka Producer and Consumer API")
public class ItemProducerController
{

    private static final String TOPIC_NAME = "Item_Topic";

    @Autowired
    private ItemRepository itemRepository;

    @Autowired
    private ItemProducerService itemProducerService;

    @Operation(summary = "Reading AMOS Item Table and publish them to the Kafka Producer.", description = "Item Messages published to Kafka", tags = { "msg" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Message Sent"),
            @ApiResponse(responseCode = "400", description = "Invalid Input"),
            @ApiResponse(responseCode = "500", description = "Service Down")})
    @PostMapping(path = "publish-items/v1", consumes = "application/json", produces = "application/text")
    public String createItemKafkaTopic()
    {
        //List<Item> items2 = itemRepository.findAll();
        List<Item> items = itemRepository.findFirst100ByItemIdIsNotNull();
        log.debug("Items size = " + (items == null ? "No Items found on DB" : items.size() + " "));

        try
        {
            itemProducerService.sendItems(items); // Send all items to Kafka
            /*// If you want to send an item one by one...
            for (Item item : items)
            {
                log.info("Item ID; Item Level; Item Descr = " + item.getItemId() + " " + item.getItemLevel() + " " + item.getItemLongDesc());
                //ToDo - to publish the item to our Kafka Topic
                itemProducerService.sendItem(item);
            }*/
        }
        catch (Exception e)
        {
            log.error("Error occurred in createItemKafkaTopic: ", e);

            return "Error occurred in createItemKafkaTopic!";
        }
        return "Items sent to Kafka Topic successfully!";
    }
}
