package za.co.woolworths.itemkafka_poc.prop;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;

@ConfigurationProperties("flink")
public class FlinkProperties {

    private KafkaProperties kafka = new KafkaProperties();
    private SqlServerProperties sqlserver = new SqlServerProperties();
    private MysqlProperties mysql = new MysqlProperties();
    private final Environment environment;


    public FlinkProperties(Environment environment) {
        this.environment = environment;
    }

    public KafkaProperties getKafka() {
        return kafka;
    }

    public void setKafka(KafkaProperties kafka) {
        this.kafka = kafka;
    }

    public SqlServerProperties getSqlserver() {
        return sqlserver;
    }

    public void setSqlserver(SqlServerProperties sqlserver) {
        this.sqlserver = sqlserver;
    }

    public MysqlProperties getMysql() {
        return mysql;
    }

    public void setMysql(MysqlProperties mysql) {
        this.mysql = mysql;
    }

    @PostConstruct
    public void init() {
        // Load MySQL credentials from environment variables
        String mysqlJdbcUrl = environment.getProperty("MYSQL_JDBC_URL");
        String mysqlUsername = environment.getProperty("MYSQL_USERNAME");
        String mysqlPassword = environment.getProperty("MYSQL_PASSWORD");


        if(mysqlJdbcUrl != null && mysqlUsername != null && mysqlPassword != null){
            mysql.setJdbcUrl(mysqlJdbcUrl);
            mysql.setUsername(mysqlUsername);
            mysql.setPassword(mysqlPassword);
        } else {
            // Handle missing environment variables appropriately - log a warning or throw an exception
            System.err.println("WARNING: MySQL environment variables not set. MySQL connection will be unavailable.");
        }

    }

    public static class KafkaProperties {
        private String bootstrapServers;
        private String topic;
        private String groupId;

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }
    }

    public static class SqlServerProperties {
        private String jdbcUrl;

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }
    }

    public static class MysqlProperties {
        private String jdbcUrl;
        private String username;
        private String password;

        public String getJdbcUrl() {
            return jdbcUrl;
        }

        public void setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }
}
