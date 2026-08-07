package com.antontech.itemkafka_poc.prop;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Type-safe binding of the {@code spring.kafka.*} configuration tree.
 * <p>
 * This is the single source of truth for Kafka connection details used by the
 * REST producer/consumer services as well as the Flink jobs, so that no class
 * needs to hardcode a bootstrap server address or topic name.
 * <p>
 * Values are supplied via {@code application.yml} (or the profile specific
 * {@code application_&lt;profile&gt;.yml}) and can be overridden per environment
 * with the {@code ITEM_KAFKA_BOOTSTRAP_SERVERS} and {@code ITEM_KAFKA_TOPIC}
 * environment variables. See {@code SETUP_GUIDE.md} for full details.
 */
@Component
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaProperties
{
    private String bootstrapServers;
    private String itemTopicName;
    private ProducerProperties producer;
    private ConsumerProperties consumer;

    /** @return the comma separated list of Kafka bootstrap servers (e.g. {@code broker1:9092,broker2:9092}). */
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    /** @param bootstrapServers the comma separated list of Kafka bootstrap servers to connect to. */
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /** @return the shared Kafka topic name used for the Item message pipeline (producer, consumer and Flink jobs). */
    public String getItemTopicName() {
        return itemTopicName;
    }

    /** @param itemTopicName the shared Kafka topic name used for the Item message pipeline. */
    public void setItemTopicName(String itemTopicName) {
        this.itemTopicName = itemTopicName;
    }

    public ProducerProperties getProducer() {
        return producer;
    }

    public void setProducer(ProducerProperties producer) {
        this.producer = producer;
    }

    public ConsumerProperties getConsumer() {
        return consumer;
    }

    public void setConsumer(ConsumerProperties consumer) {
        this.consumer = consumer;
    }

    public static class ProducerProperties {
        private String requestTimeoutMs;
        private int retries;
        private String keySerializer;
        private String valueSerializer;

        // Getters and Setters
        public String getRequestTimeoutMs() {
            return requestTimeoutMs;
        }

        public void setRequestTimeoutMs(String requestTimeoutMs) {
            this.requestTimeoutMs = requestTimeoutMs;
        }

        public int getRetries() {
            return retries;
        }

        public void setRetries(int retries) {
            this.retries = retries;
        }

        public String getKeySerializer() {
            return keySerializer;
        }

        public void setKeySerializer(String keySerializer) {
            this.keySerializer = keySerializer;
        }

        public String getValueSerializer() {
            return valueSerializer;
        }

        public void setValueSerializer(String valueSerializer) {
            this.valueSerializer = valueSerializer;
        }
    }

    public static class ConsumerProperties {
        private String groupId;
        private String keyDeserializer;
        private String valueDeserializer;
        private String autoOffsetReset;
        private int maxPollIntervalMs;
        private int sessionTimeoutMs;

        // Getters and Setters
        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public String getKeyDeserializer() {
            return keyDeserializer;
        }

        public void setKeyDeserializer(String keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
        }

        public String getValueDeserializer() {
            return valueDeserializer;
        }

        public void setValueDeserializer(String valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
        }

        public String getAutoOffsetReset() {
            return autoOffsetReset;
        }

        public void setAutoOffsetReset(String autoOffsetReset) {
            this.autoOffsetReset = autoOffsetReset;
        }

        public int getMaxPollIntervalMs() {
            return maxPollIntervalMs;
        }

        public void setMaxPollIntervalMs(int maxPollIntervalMs) {
            this.maxPollIntervalMs = maxPollIntervalMs;
        }

        public int getSessionTimeoutMs() {
            return sessionTimeoutMs;
        }

        public void setSessionTimeoutMs(int sessionTimeoutMs) {
            this.sessionTimeoutMs = sessionTimeoutMs;
        }
    }
}
