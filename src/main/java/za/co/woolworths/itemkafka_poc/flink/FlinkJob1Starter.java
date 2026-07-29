package za.co.woolworths.itemkafka_poc.flink;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import za.co.woolworths.itemkafka_poc.flink.jobs.FlinkJob1;
import za.co.woolworths.itemkafka_poc.prop.FlinkProperties;

@Component
public class FlinkJob1Starter {

    @Autowired
    private FlinkProperties flinkProperties;

    public void startJob() throws Exception {
        String[] flinkArgs = {
                "--sqlserver.jdbc.url", flinkProperties.getSqlserver().getJdbcUrl(),
                "--kafka.bootstrap.servers", flinkProperties.getKafka().getBootstrapServers(),
                "--kafka.topic", flinkProperties.getKafka().getTopic()
        };
        //FlinkJob1.main(flinkArgs);
    }
}
