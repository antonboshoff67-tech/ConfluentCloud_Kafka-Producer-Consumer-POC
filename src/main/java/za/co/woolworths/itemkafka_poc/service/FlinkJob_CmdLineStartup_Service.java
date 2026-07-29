package za.co.woolworths.itemkafka_poc.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import za.co.woolworths.itemkafka_poc.flink.jobs.FlinkJob1;
import za.co.woolworths.itemkafka_poc.flink.jobs.FlinkJob2_cmdLine;

@Component
@Slf4j
public class FlinkJob_CmdLineStartup_Service implements CommandLineRunner
{
    private final FlinkJobService flinkJobService;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers; // = "localhost:9092";

    @Value("${spring.mysql.jdbcUrl}")
    private String mysqlJdbcUrl; // = "jdbc:mysql://localhost:3306/cs_caissa_central_master_data?useSSL=false&allowPublicKeyRetrieval=true";

    @Value("${spring.mysql.username}")
    private String mysqlUsername; // = "root";

    @Value("${spring.mysql.password}")
    private String mysqlPassword; // = "Anton@Perfect07";

    public FlinkJob_CmdLineStartup_Service(FlinkJobService flinkJobService) {
        this.flinkJobService = flinkJobService;
    }

    @Override
    public void run(String... args) throws Exception
    {
        // Testing MySQL connection
       /* boolean isConnectionSuccessful = TestMySQLConnection.testConnection();
        if (isConnectionSuccessful)
        {
            System.out.println("MySQL Connection successful!");
        }
        else
        {
            System.err.println("MySQL Connection failed!");
        }*/

        // Optionally start any of the jobs on startup. Uncomment as needed.
        // flinkJobService.runSimpleJob();      // Run Simple Job
        // flinkJobService.runSimpleJob_cmdLine();      // Run Simple Job
        // flinkJobService.runJob1();         // Uncomment to run Flink Job 1
        // flinkJobService.runJob2();         // Uncomment to run Flink Job 2
        // flinkJobService.runJob1_cmdLine(); // Uncomment to run Flink Job 1 _CmdLine

        try
        {
            //FlinkJob2.main(new String[]{}); // Execute without creating a new environment
            FlinkJob2_cmdLine flinkJob2_cmdLine = new FlinkJob2_cmdLine(bootstrapServers, mysqlJdbcUrl, mysqlUsername, mysqlPassword); // Pass parameters here
            flinkJob2_cmdLine.run(); // Execute Flink Job 1
        }
        catch (Exception e)
        {
            log.error("Error running Flink Job 1: {}", e.getMessage(), e);
        }

    }
}
