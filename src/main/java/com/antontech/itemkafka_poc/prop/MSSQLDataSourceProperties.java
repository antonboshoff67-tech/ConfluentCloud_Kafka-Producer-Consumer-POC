package com.antontech.itemkafka_poc.prop;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Type-safe binding of the {@code spring.datasource.*} configuration tree.
 * <p>
 * Holds the source MS SQL Server connection details (the Item master-data
 * database) and the source table name used by the Flink
 * "MS SQL to Kafka" job ({@code MssqlItemToKafkaJob}). The JDBC URL is
 * environment-variable driven via {@code ITEM_MSSQL_URL} - see the
 * {@code SETUP_GUIDE.md} document for how to configure this for your own SQL
 * Server instance and Windows-integrated-security / certificate setup.
 */
@Component
@ConfigurationProperties(prefix = "spring.datasource")
public class MSSQLDataSourceProperties {
    private String url;
    private String driverClassName;
    private String sourceTableName;

    /** @return the full MS SQL Server JDBC connection string (including instance, database and security options). */
    public String getUrl() {
        return url;
    }
    public void setUrl(String url) {
        this.url = url;
    }
    /** @return the fully qualified MS SQL Server JDBC driver class name. */
    public String getDriverClassName() {
        return driverClassName;
    }
    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }
    /** @return the source table name to read Item rows from (defaults to {@code ITEM}). */
    public String getSourceTableName() {
        return sourceTableName;
    }
    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }
}


