package com.antontech.itemkafka_poc.controller;

import lombok.extern.slf4j.Slf4j; // Use Lombok's Slf4j for logging
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.antontech.itemkafka_poc.service.FlinkJobService;
import com.antontech.itemkafka_poc.service.JobStatus;

import java.util.concurrent.CompletableFuture;

/**
 * REST entry points used to kick off the two Item-pipeline Flink jobs and
 * the lightweight demo job, and to poll their last-known status.
 * <p>
 * See {@code API_DOCUMENTATION.md} at the repository root for full request /
 * response examples and {@code ARCHITECTURE.md} for how these jobs fit into
 * the overall MS SQL Server -&gt; Kafka -&gt; MySQL pipeline.
 */
@Slf4j // Automatically creates a logger instance
@RestController
@RequestMapping("/flink")
public class FlinkJobController
{
    private final FlinkJobService flinkJobService;

    /** @param flinkJobService service that owns and tracks the Flink job executions. */
    public FlinkJobController(FlinkJobService flinkJobService)
    {
        this.flinkJobService = flinkJobService;
    }

    /**
     * Triggers Flink Job 1 (MS SQL Server -&gt; Kafka) asynchronously in a
     * background thread and returns immediately.
     *
     * @return HTTP 200 with a confirmation message once the job has been submitted for execution (not once it has completed).
     */
    @PostMapping("/start-job1")
    public ResponseEntity<String> triggerFlinkJob1()
    {
        // Run asynchronously in the background
        CompletableFuture.runAsync(() ->
        {
            try
            {
                flinkJobService.runJob1(); // Start Flink Job 1
                log.debug("Flink Job 1 executed successfully.");
            } catch (Exception e) {
                log.error("Error starting Job 1: {}", e.getMessage(), e);
            }
        });
        return ResponseEntity.ok("Flink Job 1 started successfully.");
    }

    /**
     * Triggers Flink Job 2 (Kafka -&gt; MySQL) asynchronously in a background
     * thread and returns immediately. This job is a long-running stream and
     * will keep consuming from Kafka until cancelled.
     *
     * @return HTTP 200 with a confirmation message once the job has been submitted for execution.
     */
    @PostMapping("/start-job2")
    public ResponseEntity<String> triggerFlinkJob2()
    {
        CompletableFuture.runAsync(() ->
        {
            try
            {
                flinkJobService.runJob2(); // Start Flink Job 2
                log.debug("Flink Job 2 executed successfully.");
            }
            catch (Exception e)
            {
                log.error("Error starting Job 2: {}", e.getMessage(), e);
            }
        });
        return ResponseEntity.ok("Flink Job 2 started successfully.");
    }

    /**
     * Triggers the lightweight, dependency-free demo/smoke-test job
     * synchronously and waits for it to complete before responding.
     *
     * @return HTTP 200 if the demo job ran successfully, otherwise HTTP 400 with an error message.
     */
    @PostMapping("/start-simple-job")
    public ResponseEntity<String> triggerFlinkSimpleJob()
    {
        try
        {
            flinkJobService.runSimpleJob(); // Start Simple Flink Job
            log.debug("Flink Simple Job executed successfully.");
            return ResponseEntity.ok("Flink Simple Job executed successfully.");
        }
        catch (Exception e)
        {
            log.error("Error starting Simple Job: {}", e.getMessage());
            return ResponseEntity.badRequest().body("Error starting Simple Job");
        }
    }

    /**
     * Returns the last known status recorded for a given job name.
     *
     * @param jobName the job display name to look up, e.g. {@code "Flink Job 1"}, {@code "Flink Job 2"} or {@code "Flink Simple Job"}.
     * @return HTTP 200 with the job's {@link JobStatus} (defaults to {@code PENDING} if the job has never been triggered).
     */
    @GetMapping("/job-status")
    public ResponseEntity<JobStatus> getJobStatus(String jobName)
    {
        JobStatus status = flinkJobService.getJobStatus(jobName);
        log.debug("Job {} status requested: {}", jobName, status);
        return ResponseEntity.ok(status);
    }
}

