package za.co.woolworths.itemkafka_poc.configuration;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import za.co.woolworths.itemkafka_poc.prop.FlinkProperties;

import java.util.Properties;

@Configuration
@EnableConfigurationProperties({FlinkProperties.class})
public class FlinkConfig
{

    @Autowired
    private FlinkProperties flinkProperties;


    /*@Bean
    public FlinkKafkaProducer<String> flinkKafkaProducer()
    {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, flinkProperties.getKafka().getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new FlinkKafkaProducer<>(flinkProperties.getKafka().getTopic(), new SimpleStringSchema(), props);
    }

    @Bean
    public FlinkKafkaConsumer<String> flinkKafkaConsumer()
    {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, flinkProperties.getKafka().getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, flinkProperties.getKafka().getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new FlinkKafkaConsumer<>(flinkProperties.getKafka().getTopic(), new org.apache.kafka.common.serialization.StringDeserializer(), props);
    }*/


    /*@Bean
    public MySqlClientBuilder sqlServerBuilder()
    {
        return MySqlClientBuilder.newBuilder()
                .setDriverName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .setJdbcUrl(flinkProperties.getSqlserver().getJdbcUrl());
    }*/
}