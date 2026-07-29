package za.co.woolworths.itemkafka_poc.flink;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import za.co.woolworths.itemkafka_poc.flink.jobs.FlinkJob2;
import za.co.woolworths.itemkafka_poc.prop.FlinkProperties;

@Component
public class FlinkJob2Starter {

    @Autowired
    private FlinkProperties flinkProperties;

    public void startJob() throws Exception {
        String[] flinkArgs = {
                "--kafka.bootstrap.servers", flinkProperties.getKafka().getBootstrapServers(),
                "--kafka.topic", flinkProperties.getKafka().getTopic(),
                "--kafka.group.id", flinkProperties.getKafka().getGroupId(),
                "--mysql.jdbc.url", flinkProperties.getMysql().getJdbcUrl(),
                "--mysql.username", flinkProperties.getMysql().getUsername(),
                "--mysql.password", flinkProperties.getMysql().getPassword()
        };
        FlinkJob2.main(flinkArgs);
    }
}
