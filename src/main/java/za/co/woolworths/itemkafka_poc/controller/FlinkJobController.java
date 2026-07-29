package za.co.woolworths.itemkafka_poc.controller;

import lombok.extern.slf4j.Slf4j; // Use Lombok's Slf4j for logging
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import za.co.woolworths.itemkafka_poc.service.FlinkJobService;
import za.co.woolworths.itemkafka_poc.service.JobStatus;

import java.util.concurrent.CompletableFuture;

@Slf4j // Automatically creates a logger instance
@RestController
@RequestMapping("/flink")
public class FlinkJobController
{
   /*@Autowired
   FlinkJobProperties flinkJobProperties;*/

    private final FlinkJobService flinkJobService;

    public FlinkJobController(FlinkJobService flinkJobService)
    {
        this.flinkJobService = flinkJobService;
    }

    // Run this job asynchronously
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

    // Run this job asynchronously
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

    // Run this job synchronously
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

    @GetMapping("/job-status")
    public ResponseEntity<JobStatus> getJobStatus(String jobName)
    {
        JobStatus status = flinkJobService.getJobStatus(jobName);
        log.debug("Job {} status requested: {}", jobName, status);
        return ResponseEntity.ok(status);
    }
}
