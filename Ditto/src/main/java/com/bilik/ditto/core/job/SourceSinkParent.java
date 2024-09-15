package com.bilik.ditto.core.job;

import com.bilik.ditto.core.common.Stoppable;
import com.bilik.ditto.core.metric.CounterAggregator;
import com.bilik.ditto.core.type.Type;
import com.bilik.ditto.core.type.TypeSpecific;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/*
    TODO - rename
 */
public abstract class SourceSinkParent<T> implements Stoppable, TypeSpecific<T> {

    protected Runnable onFinish;

    public void setOnFinish(Runnable onFinish) {
        this.onFinish = onFinish;
    }

    /**
     * @return true is source was successfully started
     */
    public abstract boolean start();

    /**
     * Does not return boolean. If anything goes wrong - throw exception describing what is wrong.
     * Boolean would tell us nothing.
     */
    public abstract Collection<Integer> initialize(Map<Integer, BlockingQueue<StreamElement<T>>> workerMap);

    public abstract CounterAggregator getCounterAggregator();

    public abstract void workerFinished(int workerId);

    public abstract Set<Integer> finishedWorkers();

    public abstract int getParallelism();

    public abstract String getJobId();

    public abstract Type<T> type();

}
