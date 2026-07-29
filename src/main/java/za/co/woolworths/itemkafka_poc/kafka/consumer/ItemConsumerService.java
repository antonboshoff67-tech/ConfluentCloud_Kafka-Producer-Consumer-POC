package za.co.woolworths.itemkafka_poc.kafka.consumer;

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
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import za.co.woolworths.itemkafka_poc.model.Item;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
public class ItemConsumerService
{

    private static final Logger log = LoggerFactory.getLogger(ItemConsumerService.class);
    private final Gson gson;
    private static final String TOPIC_NAME = "Item_Topic";
    private static final String ITEM_AUTO_GROUP = "item_group";
    private static final String ITEM_MANUAL_GROUP = "manual-item-group";

    private static int autoItemGroupCounter = 0;
    private static int manualItemGroupCounter = 0;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, String> manualKafkaListenerContainerFactory;

    private boolean consumerActive = false;

    public ItemConsumerService()
    {
        // Initialize Gson with the LocalDateTimeAdapter for handling LocalDateTime serialization
        this.gson = new GsonBuilder()
                .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
                .create();
    }

    // Auto consumer for item_group - uncomment to demo Kafka consumer
    /*@KafkaListener(topics = TOPIC_NAME, groupId = ITEM_AUTO_GROUP, containerFactory = "kafkaListenerContainerFactory")
    public void autoItemGroupListen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment)
    {
        autoItemGroupCounter++;
        log.info("autoItemGroupCounter = " + autoItemGroupCounter);
        log.info("Auto consumed message with Key: {} and Value: {} and groupId {}", record.key(), record.value(),ITEM_AUTO_GROUP);
        processMessage(record, acknowledgment, ITEM_AUTO_GROUP);
    }

    // Manual consumer for manual-item-group - - uncomment to demo Kafka consumer
    @KafkaListener(topics = TOPIC_NAME, groupId = ITEM_MANUAL_GROUP, containerFactory = "manualKafkaListenerContainerFactory")
    public void autoManualItemGroupListen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment)
    {
        manualItemGroupCounter++;
        log.info("manualItemGroupCounter = " + manualItemGroupCounter);
        log.info("Manual consumer consumed message with Key: {} and Value: {} and groupId {}", record.key(), record.value(), ITEM_MANUAL_GROUP);
        processMessage(record, acknowledgment, ITEM_MANUAL_GROUP);
    }*/

    // Called from ItemConsumerController using Curl cmd... no automatic consumption of Kafka Item Objects
    // curl -X POST "http://localhost:8081/item-kafka/consumer/manual-consume/v1" -H "Content-Type: application/json" -H "Accept: application/text" -d "{\"msg\": \"Consuming Item Messages\"}"
    public String manualConsume(String groupId)
    {
        List<Item> processedItems = new ArrayList<>();

        // Create a new KafkaConsumer using the manualKafkaListenerContainerFactory
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(manualKafkaListenerContainerFactory.getConsumerFactory().getConfigurationProperties()))
        {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME)); // Subscribe to the topic

            log.info("Manual consumer subscribed to topic: {}", TOPIC_NAME);
            long startTime = System.currentTimeMillis();
            long timeoutMillis = 30000; // Set a timeout for polling, e.g., 30 seconds

            while (System.currentTimeMillis() - startTime < timeoutMillis)
            {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); // Poll for records

                log.info("Polling for records...");

                // Process each record if available
                if (records.isEmpty())
                {
                    log.warn("No records found during polling.");
                    break; // Exit if no records are found
                }

                for (ConsumerRecord<String, String> record : records)
                {
                    try
                    {
                        // Split the key to retrieve the groupId and itemId
                        //String[] keyParts = record.key().split("_"); // Expecting key pattern as "groupId_itemId"
                        //String msgGroupId = keyParts[0]; // e.g., "manual-item-group"
                        //String itemId = keyParts[1]; // Extract itemId from key

                        // Deserialize JSON to Item using the record's value
                        Item item = gson.fromJson(record.value(), Item.class);
                        log.info("Manually consumed message with Key: {} and Value: {}", record.key(), record.value());
                        log.debug("Item consumed with ID: {}", item.getItemId());

                        processedItems.add(item); // Store the processed item

                        // Here, you can implement conditional logic based on msgGroupId if needed
                        if (ITEM_MANUAL_GROUP.equals(groupId))
                        {
                            // Additional processing for items meant for manual-item-group
                            log.info("Processing item for manual-item-group.");
                            // Commit offsets after processing
                            consumer.commitSync(); // Commit offset after processing
                            log.info("Offsets committed after processing records for key: {}", record.key());
                        }
                    }
                    catch (Exception e)
                    {
                        log.error("Error processing message: {}", e.getMessage());
                        // Handle exception appropriately (e.g., log, skip message, etc.)
                    }
                }
            }

            // Return result based on processed items
            return processedItems.isEmpty() ? "No records found." : "Manually consumed " + processedItems.size() + " items.";

        }
        catch (Exception e)
        {
            log.error("Error in manual consuming: {}", e.getMessage());
            return "Error occurred during manual consumption.";
        }
    }
    private void processMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment, String consumerGroup)
    {
        try
        {
            // Parse the key to differentiate processing logic
            String[] keyParts = record.key().split("_"); // assuming your key is structured as "groupId_itemId"
            String itemId = keyParts[1]; // "item_group" or "manual-item-group"
            String msgGroupId = keyParts[0]; // Extract itemId
            String value = record.value();
            String rec = record.toString();

            Item item = gson.fromJson(record.value(), Item.class);
            log.debug("Kafka Consumed Item Id: {}", item.getItemId());
            log.debug("Kafka Consumed Item details: {}", item);

            // Add logic here to process differently based on the groupId
            if (ITEM_AUTO_GROUP.equals(consumerGroup))
            {
                // Processing logic for item_group
                // e.g., save to DB, enqueue further processing etc.
                log.info("Processing with auto-consumer logic for item group named " + ITEM_AUTO_GROUP);
                // Acknowledge the item_group message if processed successfully
                acknowledgment.acknowledge();
                log.info(ITEM_AUTO_GROUP + " Message processed and offset committed for key: {}", record.key());
            }
            else if (ITEM_MANUAL_GROUP.equals(consumerGroup))
            {
                // Processing logic for manual-item-group
                log.info("Processing with manual-consumer logic with manual item group named " + ITEM_MANUAL_GROUP);
                // Acknowledge the message if processed successfully
                acknowledgment.acknowledge();
                log.info(ITEM_MANUAL_GROUP + " Message processed and offset committed for key: {}", record.key());
            }
        }
        catch (Exception e)
        {
            log.error("Error processing message: {}", e.getMessage());
        }
    }

    // Used to check if consumer is running
    public boolean isRunning()
    {
        return consumerActive;
    }

    // Old methods Not using GrouId plus ItemId to consume Items from Item-Topic
    // ==============================================================================================================================
    /*// Auto consumer for item_group
    @KafkaListener(topics = TOPIC_NAME, groupId = "item_group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment)
    {
        log.info("Consumed message with Key: {} and Value: {}", record.key(), record.value());
        processMessage(record, acknowledgment);
    }

    // Manual consumer for manual-item-group
    @KafkaListener(topics = TOPIC_NAME, groupId = "manual-item-group", containerFactory = "manualKafkaListenerContainerFactory")
    public void manualListen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment)
    {
        log.info("Manual consumer consumed message with Key: {} and Value: {}", record.key(), record.value());
        processMessage(record, acknowledgment);
    }

    private void processMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment)
    {
        try
        {
            Item item = gson.fromJson(record.value(), Item.class);
            log.debug("Kafka Consumed Item Id: {}", item.getItemId());
            log.debug("Kafka Consumed Item details: {}", item);

            // Acknowledge the message if processed successfully
            acknowledgment.acknowledge();
            log.info("Message processed and offset committed manually for key: {}", record.key());
        }
        catch (Exception e)
        {
            log.error("Error processing message: {}", e.getMessage());
        }
    }*/

    // Method to manually consume items called from the controller
    // used to manually consume a message from Kafka using controller endpoint
    // with enough time to poll from Kafka to process all 100 items:
    // "http://localhost:8081/item-kafka/app/consume-items/v1"
    /*public String manualConsume() {
        List<Item> processedItems = new ArrayList<>();

        // Create a new KafkaConsumer using the manualKafkaListenerContainerFactory
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(manualKafkaListenerContainerFactory.getConsumerFactory().getConfigurationProperties())) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME)); // Subscribe to the topic

            log.info("Manual consumer subscribed to topic: {}", TOPIC_NAME);
            long startTime = System.currentTimeMillis();
            long timeoutMillis = 30000; // Set a timeout for polling, e.g., 30 seconds

            while (System.currentTimeMillis() - startTime < timeoutMillis)
            {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); // Poll for records

                log.info("Polling for records...");

                // Process each record if available
                if (records.isEmpty())
                {
                    log.warn("No records found during polling.");
                    //break; // Exit if no records are found
                }
                else
                {
                    for (ConsumerRecord<String, String> record : records)
                    {
                        try
                        {
                            // Deserialize JSON to Item
                            Item item = gson.fromJson(record.value(), Item.class);
                            log.info("Manually consumed message with Key: {} and Value: {}", record.key(), record.value());
                            log.debug("Kafka Consumed Item Id: {}", item.getItemId());
                            log.debug("Kafka Consumed Item details: {}", item);
                            processedItems.add(item); // Store the processed item

                            // Acknowledgment not needed in manual consumption context as Kafka sinks directly
                            // Commit offsets after processing
                            consumer.commitSync(); // Commit offset after processing
                            log.info("Offsets committed after processing records.");

                        }
                        catch (Exception e)
                        {
                            log.error("Error processing message: {}", e.getMessage());
                            // Handle exception appropriately (e.g., log, skip message, etc.)
                        }
                    }
                }
            }

            // Return result based on processed items
            if (processedItems.isEmpty())
            {
                return "No records found.";
            }
            return "Manually consumed " + processedItems.size() + " items.";
        }
        catch (Exception e)
        {
            log.error("Error in manual consuming: {}", e.getMessage());
            return "Error occurred during manual consumption.";
        }
    }*/
    // ==============================================================================================================================

    // Example placeholder for saving the item to a database, if needed
    // @Autowired
    // private YourItemRepository itemRepository;

    // private void saveItem(Item item) {
    //     itemRepository.save(item);
    // }

    // Summary of the above methods and when to use what method to consume recs from Kafka
    // -----------------------------------------------------------------------------------------------------
    // See Word doc named "14 - How to create manual and auto consumer Kafka consumer methods in Springboot differentiating between 2 item groups on message keys plus groupIds.docx"
    // that explains these methods in detail how they work

}
