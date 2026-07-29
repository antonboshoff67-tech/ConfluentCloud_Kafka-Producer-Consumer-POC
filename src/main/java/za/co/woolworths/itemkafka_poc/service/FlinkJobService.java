package za.co.woolworths.itemkafka_poc.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import za.co.woolworths.itemkafka_poc.flink.jobs.*;
import za.co.woolworths.itemkafka_poc.prop.KafkaProperties;
import za.co.woolworths.itemkafka_poc.prop.MSSQLDataSourceProperties;
import za.co.woolworths.itemkafka_poc.prop.MySqlProperties;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class FlinkJobService
{

    private KafkaProperties kafkaProperties;

    private MySqlProperties mySqlProperties;

    private MSSQLDataSourceProperties mssqlDataSourceProperties;
    private StreamExecutionEnvironment env;

    @Autowired
    public FlinkJobService(KafkaProperties kafkaProperties,
                           MySqlProperties mySqlProperties,
                           MSSQLDataSourceProperties mssqlDataSourceProperties)
    {
        this.kafkaProperties = kafkaProperties;
        this.mySqlProperties = mySqlProperties;
        this.mssqlDataSourceProperties = mssqlDataSourceProperties;

        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    // Map to track job statuses
    private final Map<String, JobStatus> jobStatuses = new HashMap<>();


    // Run Flink Job 1 with synchronization
    // public synchronized void runJob1()
    public void runJob1()
    {
        runFlinkJobWithLogging(() ->
        {
            try
            {
                //FlinkJob1.main(new String[]{}); // Execute without creating a new environment
                FlinkJob1 flinkJob1 = new FlinkJob1(mssqlDataSourceProperties.getUrl(), kafkaProperties.getBootstrapServers()); // Pass parameters here
                flinkJob1.run(); // Execute Flink Job 1
            }
            catch (Exception e)
            {
                log.error("Error running Flink Job 1: {}", e.getMessage(), e);
            }
        }, "Flink Job 1");
    }

    // Run Flink Job 2 with synchronization
    // public synchronized void runJob2()
    public void runJob2()
    {
        runFlinkJobWithLogging(() ->
        {
            try
            {
                FlinkJob2.main(new String[]{}); // Execute without creating a new environment
            }
            catch (Exception e)
            {
                log.error("Error running Flink Job 2: {}", e.getMessage(), e);
            }
        }, "Flink Job 2");
    }

    // Run Simple Flink Job with synchronization
    public synchronized void runSimpleJob()
    {
        runFlinkJobWithLogging(() ->
        {
            try
            {
                FlinkJobSimpleSample.main(new String[]{}); // Execute without creating a new environment
            }
            catch (Exception e)
            {
                log.error("Error running Simple Flink Job: {}", e.getMessage(), e);
            }
        }, "Flink Simple Job");
    }

    // Run command line Flink Job 1
    public synchronized void runJob1_cmdLine()
    {
        runFlinkJobWithLogging(() ->
        {
            try
            {
                FlinkJob1_cmdLine.main(new String[]{}); // Run command line job
            }
            catch (Exception e)
            {
                log.error("Error running Flink Job 1_ CmdLine: {}", e.getMessage(), e);
            }
        }, "Flink Job 1 CmdLine");
    }

    // Run command line Flink Job 2
    //public synchronized void runJob2_cmdLine()
    public void runJob2_cmdLine()
    {
        runFlinkJobWithLogging(() ->
        {
            try
            {
                //FlinkJob1.main(new String[]{}); // Execute without creating a new environment
                FlinkJob2_cmdLine flinkJob2_cmdLine = new FlinkJob2_cmdLine(kafkaProperties.getBootstrapServers(), mySqlProperties.getJdbcUrl(), mySqlProperties.getUsername(), mySqlProperties.getPassword()); // Pass parameters here
                flinkJob2_cmdLine.run(); // Execute Flink Job 1
            }
            catch (Exception e)
            {
                log.error("Error running Flink Job 2 CmdLine: {}", e.getMessage(), e);
            }
        }, "Flink Job 2 CmdLine");
    }

    public synchronized void runSimpleJob_cmdLine()
    {
        runFlinkJobWithLogging(() ->
        {
            try
            {
                FlinkJobSimpleSample_cmdLine.main(new String[]{}); // Execute without creating a new environment
            }
            catch (Exception e)
            {
                log.error("Error running Simple Flink Job: {}", e.getMessage(), e);
            }
        }, "Flink Simple Job");
    }

    private void runFlinkJobWithLogging(Runnable job, String jobName)
    //private synchronized void runFlinkJobWithLogging(Runnable job, String jobName)
    {
        try
        {
            updateJobStatus(jobName, JobStatus.RUNNING);
            job.run();  // Execute the job
            updateJobStatus(jobName, JobStatus.COMPLETED);
        }
        catch (Exception e)
        {
            updateJobStatus(jobName, JobStatus.FAILED);
            log.error("Execution failed for {}: {}", jobName, e.getMessage(), e);
        }
    }

    private void updateJobStatus(String jobName, JobStatus status)
    {
        jobStatuses.put(jobName, status);
        log.info("Job {} is now {}", jobName, status);
    }

    public JobStatus getJobStatus(String jobName) {
        return jobStatuses.getOrDefault(jobName, JobStatus.PENDING);
    }
}
