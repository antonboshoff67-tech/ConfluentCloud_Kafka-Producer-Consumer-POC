package za.co.woolworths.itemkafka_poc.kafka.producer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cglib.proxy.Callback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import za.co.woolworths.itemkafka_poc.kafka.consumer.LocalDateTimeAdapter;
import za.co.woolworths.itemkafka_poc.model.Item;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
public class ItemProducerService
{
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC_NAME = "Item_Topic";

    private static final String ITEM_AUTO_GROUP = "item_group";
    private static final String ITEM_MANUAL_GROUP = "manual-item-group";

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
            .create(); // Ensure LocalDateTimeAdapter is used for both serialization and deserialization
    // Distinguishing between consumers using GroupId
    public void sendItems(List<Item> items)
    {
        int totalItems = items.size();
        int midpoint = totalItems / 2;

        // Send first half to item_group (normal consumer)
        for (int i = 0; i < midpoint; i++)
        {
            sendItemWithGroupId(items.get(i), ITEM_AUTO_GROUP);
        }

        // Send second half to manual-item-group
        for (int i = midpoint; i < totalItems; i++)
        {
            sendItemWithGroupId(items.get(i), ITEM_MANUAL_GROUP);
        }
    }

    private void sendItemWithGroupId(Item item, String groupId)
    {
        try
        {
            String jsonItem = gson.toJson(item);
            // Send to the topic with the specified key that indicates group
            kafkaTemplate.send(TOPIC_NAME, groupId + "_" + item.getItemId(), jsonItem).get(); // Use item ID combined with group ID to differentiate.
            log.info("Message sent with group ID: {}", groupId);
        }
        catch (Exception e)
        {
            log.error("Failed to send message for itemId: {} due to error: {}", item.getItemId(), e.getMessage());
        }
    }

   /* // Method to send a single item to Kafka - not using GroupId's to differentiate between Consumer/GroupIds
    public void sendItem(Item item)
    {
        try
        {
            // Note: this will block until message is acknowledged
            String jsonItem = gson.toJson(item); // Serialize Item to JSON
            kafkaTemplate.send(TOPIC_NAME, item.getItemId(), jsonItem).get();
            log.debug("Kafka Published Item Id: {}", item.getItemId());
            log.debug("Kafka Published Item details: {}", item);
            log.info("*****Message sent successfully for itemId: {}*****", item.getItemId());
        }
        catch (Exception e)
        {
            log.error("Failed to send message for itemId: {} due to error: {}", item.getItemId(), e.getMessage());
        }
    }

    // Method to send a list of items to Kafka
    public void sendItems(List<Item> items) {
        for (Item item : items) {
            sendItem(item); // Use the existing sendItem method for each item
        }
    }*/

}
