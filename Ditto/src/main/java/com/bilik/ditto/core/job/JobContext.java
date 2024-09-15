package com.bilik.ditto.core.job;

import com.bilik.ditto.core.callback.JobCallback;
import com.bilik.ditto.core.metric.CounterAggregator;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * TODO: NOT IMPLEMENTED YET
 *
 * Shared object across all workers within the same job. It contains info about
 * parallelism and jobID, which are the same for the entire job.
 * Simplifies registration of metrics and callbacks.
 */
public class JobContext {

    private final String jobId;
    private volatile int parallelism;
    private final CounterAggregator counterAggregator;
    private Set<JobCallback> callbacks = new CopyOnWriteArraySet<>();

    public JobContext(String jobId,
                      int parallelism,
                      CounterAggregator counterAggregator) {
        this.jobId = jobId;
        this.parallelism = parallelism;
        this.counterAggregator = counterAggregator;
    }

    public void overWriteParallelism(int newParallelism) {
        this.parallelism = newParallelism;
    }

    public String getJobId() {
        return null;
    }

    public CounterAggregator getCounterAggregator() {
        return null;
    }

    public void addCallback(JobCallback callback) {
        callbacks.add(callback);
    }

    public Collection<JobCallback> getCallbacks() {
        return Collections.unmodifiableSet(callbacks);
    }

}
