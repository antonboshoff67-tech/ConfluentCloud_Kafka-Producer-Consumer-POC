package com.antontech.itemkafka_poc.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.antontech.itemkafka_poc.flink.jobs.FlinkWordStreamDemoJob;
import com.antontech.itemkafka_poc.flink.jobs.KafkaItemToMysqlJob;
import com.antontech.itemkafka_poc.flink.jobs.MssqlItemToKafkaJob;
import com.antontech.itemkafka_poc.prop.KafkaProperties;
import com.antontech.itemkafka_poc.prop.MSSQLDataSourceProperties;
import com.antontech.itemkafka_poc.prop.MySqlProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Orchestrates the three Flink jobs exposed by
 * {@link com.antontech.itemkafka_poc.controller.FlinkJobController}
 * and tracks their last-known {@link JobStatus} in-memory so it can be
 * queried back via {@code GET /flink/job-status}.
 * <p>
 * All JDBC/Kafka connection details are resolved from the injected
 * {@link KafkaProperties}, {@link MySqlProperties} and
 * {@link MSSQLDataSourceProperties} beans (which are themselves bound from
 * {@code application.yml} / environment variables) - no credential or
 * connection string is hardcoded here or in the underlying job classes.
 */
@Slf4j
@Service
public class FlinkJobService
{

    private final KafkaProperties kafkaProperties;

    private final MySqlProperties mySqlProperties;

    private final MSSQLDataSourceProperties mssqlDataSourceProperties;
    private final StreamExecutionEnvironment env;

    /**
     * @param kafkaProperties           Kafka connection/topic settings shared by both jobs.
     * @param mySqlProperties           MySQL connection/table settings used by {@link KafkaItemToMysqlJob}.
     * @param mssqlDataSourceProperties MS SQL Server connection/table settings used by {@link MssqlItemToKafkaJob}.
     */
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

    /** Tracks the last known status of every job that has been triggered, keyed by job name. */
    private final Map<String, JobStatus> jobStatuses = new HashMap<>();

    /**
     * Runs the MS SQL Server -&gt; Kafka job ({@link MssqlItemToKafkaJob}).
     * Reads a batch of Item rows from the configured source table and
     * publishes them as JSON messages to the configured Kafka topic.
     * Updates the tracked {@link JobStatus} for {@code "Flink Job 1"}.
     */
    public void runJob1()
    {
        runFlinkJobWithLogging(() ->
        {
            try
            {
                MssqlItemToKafkaJob job = new MssqlItemToKafkaJob(
                        mssqlDataSourceProperties.getUrl(),
                        kafkaProperties.getBootstrapServers(),
                        kafkaProperties.getItemTopicName(),
                        mssqlDataSourceProperties.getSourceTableName());
                job.run();
            }
            catch (Exception e)
            {
                log.error("Error running MssqlItemToKafkaJob: {}", e.getMessage(), e);
            }
        }, "Flink Job 1");
    }

    /**
     * Runs the Kafka -&gt; MySQL job ({@link KafkaItemToMysqlJob}).
     * Continuously consumes Item JSON messages from the configured Kafka
     * topic and upserts them into the configured MySQL table.
     * Updates the tracked {@link JobStatus} for {@code "Flink Job 2"}.
     */
    public void runJob2()
    {
        runFlinkJobWithLogging(() ->
        {
            try
            {
                KafkaItemToMysqlJob job = new KafkaItemToMysqlJob(
                        kafkaProperties.getBootstrapServers(),
                        kafkaProperties.getItemTopicName(),
                        kafkaProperties.getConsumer().getGroupId(),
                        mySqlProperties.getJdbcUrl(),
                        mySqlProperties.getUsername(),
                        mySqlProperties.getPassword(),
                        mySqlProperties.getItemTableName());
                job.run();
            }
            catch (Exception e)
            {
                log.error("Error running KafkaItemToMysqlJob: {}", e.getMessage(), e);
            }
        }, "Flink Job 2");
    }

    /**
     * Runs the lightweight, dependency-free {@link FlinkWordStreamDemoJob}
     * smoke test. Useful to verify the Flink runtime works before
     * troubleshooting the real Item pipeline jobs.
     * Updates the tracked {@link JobStatus} for {@code "Flink Simple Job"}.
     */
    public synchronized void runSimpleJob()
    {
        runFlinkJobWithLogging(() ->
        {
            try
            {
                FlinkWordStreamDemoJob.main(new String[]{});
            }
            catch (Exception e)
            {
                log.error("Error running FlinkWordStreamDemoJob: {}", e.getMessage(), e);
            }
        }, "Flink Simple Job");
    }

    /**
     * Runs the given job while tracking its {@link JobStatus} transitions
     * (RUNNING -&gt; COMPLETED/FAILED) so that {@link #getJobStatus(String)}
     * reflects the outcome.
     *
     * @param job     the job body to execute.
     * @param jobName the display name used as the status map key and in log messages.
     */
    private void runFlinkJobWithLogging(Runnable job, String jobName)
    {
        try
        {
            updateJobStatus(jobName, JobStatus.RUNNING);
            job.run();
            updateJobStatus(jobName, JobStatus.COMPLETED);
        }
        catch (Exception e)
        {
            updateJobStatus(jobName, JobStatus.FAILED);
            log.error("Execution failed for {}: {}", jobName, e.getMessage(), e);
        }
    }

    /**
     * Records the current status for a job.
     *
     * @param jobName the job's display name.
     * @param status  the new {@link JobStatus} to record.
     */
    private void updateJobStatus(String jobName, JobStatus status)
    {
        jobStatuses.put(jobName, status);
        log.info("Job {} is now {}", jobName, status);
    }

    /**
     * @param jobName the job's display name, e.g. {@code "Flink Job 1"}, {@code "Flink Job 2"} or {@code "Flink Simple Job"}.
     * @return the last known {@link JobStatus} for that job, or {@link JobStatus#PENDING} if it has never been run.
     */
    public JobStatus getJobStatus(String jobName) {
        return jobStatuses.getOrDefault(jobName, JobStatus.PENDING);
    }
}

