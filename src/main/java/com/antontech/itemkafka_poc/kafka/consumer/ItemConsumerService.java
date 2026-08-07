package com.antontech.itemkafka_poc.kafka.consumer;

import com.antontech.itemkafka_poc.model.Item;
import com.antontech.itemkafka_poc.prop.KafkaProperties;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Provides an on-demand ("manual") Kafka consumer for the shared Item topic,
 * used by {@link com.antontech.itemkafka_poc.controller.ItemConsumerController}
 * to demonstrate polling for messages outside of a {@code @KafkaListener}.
 */
@Service
public class ItemConsumerService {

    private static final Logger log = LoggerFactory.getLogger(ItemConsumerService.class);
    private static final long POLL_TIMEOUT_MILLIS = 30000;

    private final Gson gson;
    private final KafkaProperties kafkaProperties;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, String> manualKafkaListenerContainerFactory;

    /** @param kafkaProperties supplies the shared Item topic name to consume from. */
    @Autowired
    public ItemConsumerService(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        this.gson = new GsonBuilder()
                .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
                .create();
    }

    /**
     * Opens a short-lived Kafka consumer using the given consumer group id,
     * polls the shared Item topic for up to {@value #POLL_TIMEOUT_MILLIS}ms
     * (stopping early once a poll returns no records), and commits offsets
     * synchronously per record.
     *
     * @param groupId the Kafka consumer group id to use for this one-off poll (e.g. {@code item_group} or {@code manual-item-group}).
     * @return a human-readable summary of how many items were consumed, or an error message.
     */
    public String manualConsume(String groupId) {
        List<Item> processedItems = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(manualKafkaListenerContainerFactory.getConsumerFactory().getConfigurationProperties())) {
            consumer.subscribe(Collections.singletonList(kafkaProperties.getItemTopicName()));
            long startTime = System.currentTimeMillis();

            while (System.currentTimeMillis() - startTime < POLL_TIMEOUT_MILLIS) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    break;
                }

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Item item = gson.fromJson(record.value(), Item.class);
                        if (item != null) {
                            processedItems.add(item);
                            log.info("Consumed item {} from Kafka group {}", item.getItemId(), groupId);
                        }
                        consumer.commitSync();
                    } catch (Exception e) {
                        log.error("Error processing message {}", record.key(), e);
                    }
                }
            }

            return processedItems.isEmpty() ? "No records found." : "Manually consumed " + processedItems.size() + " items.";
        } catch (Exception e) {
            log.error("Error in manual consuming", e);
            return "Error occurred during manual consumption.";
        }
    }

    /**
     * @return whether an always-on {@code @KafkaListener} consumer is currently registered/active.
     * This POC does not currently register a continuous listener, so this always returns {@code false};
     * it is a placeholder for wiring in a real listener health check.
     */
    public boolean isRunning() {
        return false;
    }
}
