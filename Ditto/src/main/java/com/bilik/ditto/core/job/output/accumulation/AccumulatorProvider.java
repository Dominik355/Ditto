package com.bilik.ditto.core.job.output.accumulation;

/**
 * used by Sink implementation to assign accumulators to workers.
 * This exists only because I need to get workerId to fileaccumulator,
 * to distinguish temp files
 */
@FunctionalInterface
public interface AccumulatorProvider<IN> {

    public SinkAccumulator<IN> createAccumulator(int workerThreadNum);

}
