package com.antontech.itemkafka_poc.prop;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Type-safe binding of the {@code spring.mysql.*} configuration tree.
 * <p>
 * Holds the MySQL/Confluent-downstream JDBC connection details and target
 * table name used by the Flink "Kafka to MySQL" sink job
 * ({@code KafkaItemToMysqlJob}). All values are environment-variable driven
 * (see {@code application.yml}) so no credential is ever hardcoded in Java.
 */
@Component
@ConfigurationProperties(prefix = "spring.mysql")
public class MySqlProperties {
    private String jdbcUrl;
    private String driverClassName;
    private String username;
    private String password;
    private String itemTableName;

    /** @return the JDBC URL of the target MySQL database, e.g. {@code jdbc:mysql://host:3306/db}. */
    public String getJdbcUrl() {
        return jdbcUrl;
    }
    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }
    /** @return the fully qualified MySQL JDBC driver class name. */
    public String getDriverClassName() {
        return driverClassName;
    }
    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }
    /** @return the MySQL username used to authenticate the JDBC connection. */
    public String getUsername() {
        return username;
    }
    public void setUsername(String username) {
        this.username = username;
    }
    /** @return the MySQL password used to authenticate the JDBC connection. */
    public String getPassword() {
        return password;
    }
    public void setPassword(String password) {
        this.password = password;
    }
    /** @return the destination table name that the Item records are upserted into (defaults to {@code ITEM}). */
    public String getItemTableName() {
        return itemTableName;
    }
    public void setItemTableName(String itemTableName) {
        this.itemTableName = itemTableName;
    }
}

