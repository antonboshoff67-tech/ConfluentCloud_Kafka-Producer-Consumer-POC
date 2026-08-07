package com.antontech.itemkafka_poc.service;

/** Lifecycle states tracked for a Flink job triggered via {@code /flink/*} endpoints. */
public enum JobStatus {
    /** The job has never been triggered (default/unknown state). */
    PENDING,
    /** The job has been submitted and is currently executing. */
    RUNNING,
    /** The job finished executing without throwing an exception. */
    COMPLETED,
    /** The job threw an exception during execution. */
    FAILED
}

