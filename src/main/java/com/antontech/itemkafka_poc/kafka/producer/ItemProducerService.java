package com.antontech.itemkafka_poc.kafka.producer;

import com.antontech.itemkafka_poc.kafka.consumer.LocalDateTimeAdapter;
import com.antontech.itemkafka_poc.model.Item;
import com.antontech.itemkafka_poc.prop.KafkaProperties;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Publishes {@link Item} records to the shared Kafka topic
 * ({@code spring.kafka.item-topic-name}, default {@code Item_Topic}).
 * <p>
 * Used by {@link com.antontech.itemkafka_poc.controller.ItemProducerController}
 * to demonstrate producing messages that are alternately tagged with the
 * auto ({@code item_group}) and manual ({@code manual-item-group}) consumer
 * group prefixes, purely so that both consumption styles exposed by
 * {@link com.antontech.itemkafka_poc.controller.ItemConsumerController} have
 * sample data to read.
 */
@Slf4j
@Service
public class ItemProducerService {

    private static final String ITEM_AUTO_GROUP = "item_group";
    private static final String ITEM_MANUAL_GROUP = "manual-item-group";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaProperties kafkaProperties;
    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
            .create();

    /**
     * @param kafkaTemplate   Spring Kafka template used to publish messages.
     * @param kafkaProperties supplies the shared Item topic name to publish onto.
     */
    public ItemProducerService(KafkaTemplate<String, String> kafkaTemplate, KafkaProperties kafkaProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * Publishes the given items to Kafka, splitting them roughly in half so
     * the first half is keyed for the "auto" consumer group and the second
     * half for the "manual" consumer group (see {@link com.antontech.itemkafka_poc.controller.ItemConsumerController}).
     *
     * @param items the items to publish; may be empty but must not be {@code null}.
     */
    public void sendItems(List<Item> items) {
        int midpoint = items.size() / 2;
        for (int i = 0; i < items.size(); i++) {
            String groupId = i < midpoint ? ITEM_AUTO_GROUP : ITEM_MANUAL_GROUP;
            sendItemWithGroupId(items.get(i), groupId);
        }
    }

    /**
     * Serializes a single item to JSON and sends it to the configured topic,
     * using {@code <groupId>_<itemId>} as the message key.
     *
     * @param item    the item to publish.
     * @param groupId logical grouping tag embedded in the message key (not the actual Kafka consumer group).
     */
    private void sendItemWithGroupId(Item item, String groupId) {
        try {
            String jsonItem = gson.toJson(item);
            kafkaTemplate.send(kafkaProperties.getItemTopicName(), groupId + "_" + item.getItemId(), jsonItem).get();
            log.info("Sent item {} to Kafka group {}", item.getItemId(), groupId);
        } catch (Exception e) {
            log.error("Failed to send item {}", item.getItemId(), e);
        }
    }
}
