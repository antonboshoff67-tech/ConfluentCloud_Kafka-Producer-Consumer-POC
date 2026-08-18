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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Provides both an always-on {@code @KafkaListener} consumer (auto/continuous
 * mode, consumer group {@code item_group}) and an on-demand ("manual") Kafka
 * consumer for the shared Item topic, used by
 * {@link com.antontech.itemkafka_poc.controller.ItemConsumerController} to
 * demonstrate both consumption styles side by side.
 */
@Service
public class ItemConsumerService {

    private static final Logger log = LoggerFactory.getLogger(ItemConsumerService.class);
    private static final long POLL_TIMEOUT_MILLIS = 30000;
    private static final String AUTO_LISTENER_ID = "itemAutoListenerContainer";

    private final Gson gson;
    private final KafkaProperties kafkaProperties;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, String> manualKafkaListenerContainerFactory;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

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
     * Always-on, continuously-running consumer for the shared Item topic, registered
     * via Spring Kafka's {@code @KafkaListener} annotation (as opposed to the manual,
     * on-demand polling done in {@link #manualConsume(String)}). Backed by the
     * {@code kafkaListenerContainerFactory} bean (consumer group {@code item_group},
     * concurrency 3, manual ack mode) defined in {@code KafkaConsumerConfig}.
     * <p>
     * Spring Kafka automatically starts this listener container on application
     * startup and keeps it running/polling in the background for the lifetime of
     * the application - no manual trigger is required.
     *
     * @param record the raw Kafka record consumed from the shared Item topic.
     * @param acknowledgment used to manually commit the offset once the record has
     *                        been processed successfully (required since the container
     *                        factory uses {@code AckMode.MANUAL}).
     */
    @KafkaListener(
            id = AUTO_LISTENER_ID,
            topics = "${spring.kafka.item-topic-name}",
            groupId = "item_group",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeItemAuto(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            Item item = gson.fromJson(record.value(), Item.class);
            if (item != null) {
                log.info("[@KafkaListener:item_group] Auto-consumed item {} (partition={}, offset={})",
                        item.getItemId(), record.partition(), record.offset());
            }
        } catch (Exception e) {
            log.error("[@KafkaListener:item_group] Error processing message {}", record.key(), e);
        } finally {
            acknowledgment.acknowledge();
        }
    }

    /**
     * @return whether the always-on {@code @KafkaListener} consumer container
     * ({@value #AUTO_LISTENER_ID}) is currently registered and actively running.
     */
    public boolean isRunning() {
        MessageListenerContainer container = kafkaListenerEndpointRegistry.getListenerContainer(AUTO_LISTENER_ID);
        return container != null && container.isRunning();
    }
}
