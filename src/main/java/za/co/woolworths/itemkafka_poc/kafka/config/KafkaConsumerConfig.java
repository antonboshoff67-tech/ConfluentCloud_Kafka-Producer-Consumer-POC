package za.co.woolworths.itemkafka_poc.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.apache.kafka.clients.consumer.*;

import java.util.HashMap;
import java.util.Map;


@Configuration
public class KafkaConsumerConfig
{
    private static final String ITEM_GROUP = "item_group";
    private static final String MANUEL_ITEM_GROUP = "manual-item-group";

    @Bean
    public ConsumerFactory<String, String> consumerFactory()
    {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Your Kafka bootstrap servers
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ITEM_GROUP); // Your consumer group ID
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Disable auto-commit
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3); // Number of concurrent consumers
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL); // If using manual acknowledgment
        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> manualConsumerFactory()
    {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, MANUEL_ITEM_GROUP); // DIFFERENT GROUP ID
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // or "latest"
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Disable auto-commit for manual consumer
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> manualKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(manualConsumerFactory());
        factory.setConcurrency(1); // Usually 1 for manual consumption
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL); // Set MANUAL AckMode
        return factory;
    }

    // Notes on how this class works...
    // This KafkaConsumerConfig class is used in my application.  It's crucial because it defines
    // the beans (consumerFactory and kafkaListenerContainerFactory) that Spring uses to create and
    // configure your Kafka consumers.
    //
    //The manualConsume() method in my ItemConsumerService uses the kafkaListenerContainerFactory
    // to get the consumer configuration.  This factory utilizes the properties you define in
    // KafkaConsumerConfig.  Therefore, we generally do not need to add additional properties directly
    // in your application.yml file under spring.kafka.consumer.properties.
    // The consumerFactory() method in my config class already sets the essential properties.
    //
    //Here's why we likely don't need extra properties in application.yml:
    //
    //bootstrap-servers: This is already set in consumerFactory().
    //group-id: This is already set in consumerFactory(). Make sure this matches the groupId in our
    // @KafkaListener annotation.
    //key-deserializer and value-deserializer: These are already configured in consumerFactory().
    //auto-offset-reset: This configures the behavior when the consumer starts; it's typically set
    // in consumerFactory().
    //When you might need additional properties in application.yml:
    //
    //We would add properties under spring.kafka.consumer.properties only for advanced settings not
    // covered by the standard settings in consumerFactory().  Examples include:
    //
    //Security: If you're using SSL or SASL/PLAIN authentication with your Kafka brokers, you'd
    // need to add security-related properties (like security.protocol, ssl.truststore.location,
    // ssl.key.password, etc.) here.
    //
    //Custom Properties:  If there are any other Kafka consumer properties specific to your
    // Kafka setup or your application's requirements (e.g., connection timeouts, specific consumer configurations not exposed through the Spring Kafka API directly).
    //
    //In summary:
    //
    //For most cases, with String deserialization and basic configurations,  you should remove
    // the spring.kafka.consumer.properties section entirely from your application.yml file.
    // The configuration within your KafkaConsumerConfig class is sufficient.  Only add properties
    // to spring.kafka.consumer.properties if you need to override or set advanced options that
    // aren't configurable via the consumerFactory().
    //
}